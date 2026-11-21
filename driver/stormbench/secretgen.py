# SPDX-License-Identifier: Apache-2.0
"""Make the benchmark's TLS Secret, in memory. (The operator, account and user keys are not made here: the provision init container of
the callout pod makes them, see components/provision.)

Nothing here touches the disk: the throwaway CA and the broker certificate come from `cryptography`, and the manifest is handed to
kubectl on stdin. The CA's private key is discarded as soon as it
has signed the broker certificate; the cluster only ever holds the CA certificate and the broker's key pair.
"""
from __future__ import annotations

import datetime as dt
import ipaddress

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

NAMESPACE = "storm-bench"
TLS_NAME = "storm-tls"
# Operator mode: made by the provision init container of the callout pod (components/provision), not by `bench secrets create`.
# The names must equal the constants in components/provision/provision.go (a test checks).
PROVISION_SECRETS = ("storm-callout-auth", "storm-generator-auth")
PROVISION_CONFIGMAP = "storm-operator"

# The names the generator and the callout use for the broker, in the namespace's DNS.
BROKER_NAMES = ("broker", f"broker.{NAMESPACE}", f"broker.{NAMESPACE}.svc", f"broker.{NAMESPACE}.svc.cluster.local", "localhost")


def _pem_key(key: ec.EllipticCurvePrivateKey) -> str:
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()).decode()


def make_tls(names: tuple[str, ...] = BROKER_NAMES, days: int = 7) -> dict[str, str]:
    """A throwaway CA and a broker certificate it signed: {"ca.pem", "tls.crt", "tls.key"} as PEM text."""
    now = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)  # a little back, for clock skew
    until = now + dt.timedelta(days=days)

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "storm-bench throwaway CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(now).not_valid_after(until)
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
                                       key_encipherment=False, data_encipherment=False, key_agreement=False,
                                       encipher_only=False, decipher_only=False), critical=True)
          .sign(ca_key, hashes.SHA256()))

    key = ec.generate_private_key(ec.SECP256R1())
    sans: list[x509.GeneralName] = []
    for n in names:
        try:
            sans.append(x509.IPAddress(ipaddress.ip_address(n)))
        except ValueError:
            sans.append(x509.DNSName(n))
    cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[0])]))
            .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now).not_valid_after(until)
            .add_extension(x509.SubjectAlternativeName(sans), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    return {"ca.pem": ca.public_bytes(serialization.Encoding.PEM).decode(),
            "tls.crt": cert.public_bytes(serialization.Encoding.PEM).decode(),
            "tls.key": _pem_key(key)}


def secret_manifest(name: str, data: dict[str, str]) -> dict:
    return {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
            "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"app.kubernetes.io/part-of": "storm-bench"}},
            "stringData": data}


def namespace_manifest() -> dict:
    return {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NAMESPACE}}


def manifests(tls: dict[str, str] | None = None) -> list[dict]:
    """The Secret the stack expects from `bench secrets create` (see deploy/README.md): the TLS key pair and its CA."""
    return [secret_manifest(TLS_NAME, tls or make_tls())]
