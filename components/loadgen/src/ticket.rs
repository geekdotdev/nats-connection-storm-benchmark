// SPDX-License-Identifier: Apache-2.0
//
// Tickets for the operator-mode broker: what the generator presents instead of a user name and password.
//
// A ticket is a NATS user JWT signed by the AUTH account's scoped signing key, with no permissions of its own and the
// role names it asks for as tags (see components/provision and components/callout). The broker verifies it, hands it to the
// auth callout, and the callout answers with the real user. Each connection has its own ticket and its own user key: the
// connection proves it holds that key by signing the nonce the broker sends in INFO.
//
// This is a small implementation of the parts of NATS nkeys and the JWT v2 format that a ticket needs, on `ring` (already a
// dependency through rustls) and `data-encoding`, instead of the nkeys crate and a JWT library. The Go library is the
// authority on the format: the golden vectors in the tests come from it, and components/callout has a test that runs this
// generator against a real nats-server.

use data_encoding::{BASE32_NOPAD, BASE64URL_NOPAD};
use ring::digest;
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;

type Res<T> = Result<T, String>;

// nkey prefix bytes (https://github.com/nats-io/nkeys): the first base32 character of a public key is the kind.
const PREFIX_BYTE_SEED: u8 = 18 << 3; // 'S'
const PREFIX_BYTE_ACCOUNT: u8 = 0; // 'A'
const PREFIX_BYTE_USER: u8 = 20 << 3; // 'U'

/// CRC-16/XMODEM, the checksum nkeys append to every encoded key (little-endian).
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc ^= (b as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
        }
    }
    crc
}

fn with_checksum(mut raw: Vec<u8>) -> String {
    let crc = crc16(&raw);
    raw.extend_from_slice(&crc.to_le_bytes());
    BASE32_NOPAD.encode(&raw)
}

/// An encoded public key of the given kind: prefix byte, 32 key bytes, checksum, in base32.
pub fn encode_public(prefix: u8, key: &[u8]) -> String {
    let mut raw = vec![prefix];
    raw.extend_from_slice(key);
    with_checksum(raw)
}

/// An encoded seed of the given kind: two bytes that carry the seed marker and the kind, 32 seed bytes, checksum.
#[cfg(test)]
fn encode_seed(prefix: u8, seed: &[u8; 32]) -> String {
    let mut raw = vec![PREFIX_BYTE_SEED | (prefix >> 5), (prefix & 31) << 3];
    raw.extend_from_slice(seed);
    with_checksum(raw)
}

/// Decodes a seed ("SA...", "SU...") and checks its checksum, that it is a seed, and that it is of the expected kind.
pub fn decode_seed(text: &str, expected_prefix: u8) -> Res<[u8; 32]> {
    let raw = BASE32_NOPAD.decode(text.trim().as_bytes()).map_err(|_| "not valid base32".to_string())?;
    if raw.len() != 36 {
        return Err(format!("a seed is 36 bytes, this is {}", raw.len()));
    }
    let (body, check) = raw.split_at(34);
    if crc16(body).to_le_bytes() != [check[0], check[1]] {
        return Err("checksum mismatch".into());
    }
    if body[0] & 0xF8 != PREFIX_BYTE_SEED {
        return Err("not a seed".into());
    }
    let kind = ((body[0] & 7) << 5) | (body[1] >> 3);
    if kind != expected_prefix {
        return Err("a seed of a different kind (an account seed is expected)".into());
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&body[2..34]);
    Ok(seed)
}

/// Checks that `text` is an account public key ("A...") with a valid checksum.
pub fn check_account_public(text: &str) -> Res<()> {
    let raw = BASE32_NOPAD.decode(text.trim().as_bytes()).map_err(|_| "not valid base32".to_string())?;
    if raw.len() != 35 || raw[0] != PREFIX_BYTE_ACCOUNT {
        return Err("not an account public key (it must start with A and be 56 characters)".into());
    }
    if crc16(&raw[..33]).to_le_bytes() != [raw[33], raw[34]] {
        return Err("checksum mismatch".into());
    }
    Ok(())
}

fn b64(bytes: &[u8]) -> String {
    BASE64URL_NOPAD.encode(bytes)
}

/// One connection's credentials: the ticket JWT and the user key that signs the broker's nonce.
pub struct Ticket {
    pub jwt: String,
    key: Ed25519KeyPair,
}

impl Ticket {
    /// The `sig` field of CONNECT: the nonce from INFO signed with this ticket's user key, in base64url without padding.
    pub fn sign_nonce(&self, nonce: &str) -> String {
        b64(self.key.sign(nonce.as_bytes()).as_ref())
    }
}

/// Makes tickets with the scoped signing key.
pub struct Issuer {
    key: Ed25519KeyPair,
    public: String,
    auth_account: String,
    roles: Vec<String>,
    ttl_secs: u64,
    rng: SystemRandom,
}

impl Issuer {
    /// `scoped_seed` is the AUTH account's scoped signing seed, `auth_account` that account's public key (the ticket's
    /// issuer_account), `roles` the role names to ask for, `ttl_secs` how long a ticket is valid for.
    pub fn new(scoped_seed: &str, auth_account: &str, roles: &[String], ttl_secs: u64) -> Res<Issuer> {
        let seed = decode_seed(scoped_seed, PREFIX_BYTE_ACCOUNT).map_err(|e| format!("the scoped seed: {e}"))?;
        check_account_public(auth_account).map_err(|e| format!("the AUTH account public key: {e}"))?;
        if ttl_secs == 0 {
            return Err("the ticket lifetime must be positive".into());
        }
        let key = Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|_| "the scoped seed is not a usable ed25519 seed".to_string())?;
        let public = encode_public(PREFIX_BYTE_ACCOUNT, key.public_key().as_ref());
        // The jwt library lowercases tags when it adds them, and the callout matches role names as they are.
        let roles = roles.iter().map(|r| r.trim().to_lowercase()).filter(|r| !r.is_empty()).collect();
        Ok(Issuer { key, public, auth_account: auth_account.trim().to_string(), roles, ttl_secs, rng: SystemRandom::new() })
    }

    /// The scoped key's public key: the ticket's `iss`.
    pub fn public(&self) -> &str {
        &self.public
    }

    /// A fresh user key and a ticket for it, issued at `now_secs` (Unix time).
    pub fn ticket(&self, name: &str, now_secs: u64) -> Res<Ticket> {
        let mut seed = [0u8; 32];
        self.rng.fill(&mut seed).map_err(|_| "no randomness".to_string())?;
        let user = Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|_| "bad user seed".to_string())?;
        let user_public = encode_public(PREFIX_BYTE_USER, user.public_key().as_ref());

        // No permissions or limits of their own: pub and sub are empty objects, and nothing else is set, because the broker
        // refuses a scoped signer's JWT that carries any. The scope's template supplies the permissions.
        let mut claims = json!({
            "exp": now_secs + self.ttl_secs,
            "iat": now_secs,
            "iss": self.public,
            "name": name,
            "sub": user_public,
            "nats": {
                "pub": {},
                "sub": {},
                "issuer_account": self.auth_account,
                "tags": self.roles,
                "type": "user",
                "version": 2,
            },
        });
        // jti: the hash of the claims without it, as the jwt library does (SHA-512/256, base32). The broker does not check it.
        let jti = BASE32_NOPAD.encode(digest::digest(&digest::SHA512_256, claims.to_string().as_bytes()).as_ref());
        claims["jti"] = json!(jti);

        let header = b64(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
        let payload = b64(claims.to_string().as_bytes());
        let signed = format!("{header}.{payload}");
        let sig = b64(self.key.sign(signed.as_bytes()).as_ref());
        Ok(Ticket { jwt: format!("{signed}.{sig}"), key: user })
    }
}

/// A new account seed in nkey text form, for tests.
#[cfg(test)]
pub fn new_account_seed() -> String {
    let mut seed = [0u8; 32];
    SystemRandom::new().fill(&mut seed).unwrap();
    encode_seed(PREFIX_BYTE_ACCOUNT, &seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{ED25519, UnparsedPublicKey};

    // Vectors made with the Go nkeys library (github.com/nats-io/nkeys v0.4.17).
    const GO_ACCOUNT_SEED: &str = "SAAG4OGNC2HOEDRURUJXZBIP66IQVBGUVT5RUS5HQUOPS7MJTGVLUEPGXE";
    const GO_ACCOUNT_PUBLIC: &str = "AB6RUQE7JN26UCECUZRWGQUZOCHDLET5GAYX5MG26XYZLKNUJFOZ7ISB";
    const GO_ACCOUNT_SIG_OF_NONCE_0: &str = "VtT1VAJ7UA_a6GbT6pYxrEA5deWKJOxqZ7wDkwJSfWh4jQp9dOu3HTd9nd9K8s5JQDQNxv-ju2dtSnAd3YgkDQ";
    const GO_USER_SEED: &str = "SUADPZWKOTODZQFI3CE5LLGURZEYRULCYY2ZMTH2EJNPTDMSSXKQIX3QSE";
    const GO_USER_PUBLIC: &str = "UBH7NLSAONBKLM3LTHDUIOOOQB23HFTLTVDM4FWMCFCEYDQ2CVTEVP3S";
    const GO_USER_SIG_OF_NONCE_1: &str = "5RklIbKe6Tn_l_Jqm4P80lcS1tp5QJdPFSmL928aThdFvyCQ0m5_1sTCjrcinHz3qJ6dP3iETZ_DE_I6V4iWDA";
    const GO_OPERATOR_SEED: &str = "SOAIE3IHZARMQXEEWVNZMOTPHGOBCC4JZDOTLYSCX4PRW6PPBDKESFPK2Q";

    #[test]
    fn crc16_is_xmodem() {
        assert_eq!(crc16(b"123456789"), 0x31C3); // the standard check value of CRC-16/XMODEM
    }

    #[test]
    fn a_seed_from_go_gives_the_same_public_key_and_signature() {
        let seed = decode_seed(GO_ACCOUNT_SEED, PREFIX_BYTE_ACCOUNT).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        assert_eq!(encode_public(PREFIX_BYTE_ACCOUNT, key.public_key().as_ref()), GO_ACCOUNT_PUBLIC);
        // ed25519 signatures are deterministic, so they must match the Go library's byte for byte.
        assert_eq!(b64(key.sign(b"storm-nonce-0").as_ref()), GO_ACCOUNT_SIG_OF_NONCE_0);

        let seed = decode_seed(GO_USER_SEED, PREFIX_BYTE_USER).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        assert_eq!(encode_public(PREFIX_BYTE_USER, key.public_key().as_ref()), GO_USER_PUBLIC);
        assert_eq!(b64(key.sign(b"storm-nonce-1").as_ref()), GO_USER_SIG_OF_NONCE_1);
    }

    #[test]
    fn seeds_round_trip_and_start_with_the_kind() {
        let seed = decode_seed(GO_ACCOUNT_SEED, PREFIX_BYTE_ACCOUNT).unwrap();
        assert_eq!(encode_seed(PREFIX_BYTE_ACCOUNT, &seed), GO_ACCOUNT_SEED);
        let seed = decode_seed(GO_USER_SEED, PREFIX_BYTE_USER).unwrap();
        assert_eq!(encode_seed(PREFIX_BYTE_USER, &seed), GO_USER_SEED);
        assert!(new_account_seed().starts_with("SA"));
    }

    #[test]
    fn bad_seeds_are_refused() {
        assert!(decode_seed(GO_USER_SEED, PREFIX_BYTE_ACCOUNT).unwrap_err().contains("different kind"));
        assert!(decode_seed(GO_OPERATOR_SEED, PREFIX_BYTE_ACCOUNT).unwrap_err().contains("different kind"));
        assert!(decode_seed(GO_ACCOUNT_PUBLIC, PREFIX_BYTE_ACCOUNT).is_err()); // a public key is not a seed
        assert!(decode_seed("", PREFIX_BYTE_ACCOUNT).is_err());
        assert!(decode_seed("not base32!", PREFIX_BYTE_ACCOUNT).is_err());
        let mut broken = GO_ACCOUNT_SEED.to_string();
        broken.replace_range(10..11, if &broken[10..11] == "A" { "B" } else { "A" });
        assert!(decode_seed(&broken, PREFIX_BYTE_ACCOUNT).unwrap_err().contains("checksum"));
    }

    #[test]
    fn account_public_keys_are_checked() {
        check_account_public(GO_ACCOUNT_PUBLIC).unwrap();
        assert!(check_account_public(GO_USER_PUBLIC).is_err());
        assert!(check_account_public("").is_err());
        assert!(check_account_public(&GO_ACCOUNT_PUBLIC[..55]).is_err());
    }

    fn issuer(roles: &[&str], ttl: u64) -> Issuer {
        let roles: Vec<String> = roles.iter().map(|s| s.to_string()).collect();
        Issuer::new(GO_ACCOUNT_SEED, GO_ACCOUNT_PUBLIC, &roles, ttl).unwrap()
    }

    fn decode_part(part: &str) -> Vec<u8> {
        BASE64URL_NOPAD.decode(part.as_bytes()).unwrap()
    }

    #[test]
    fn a_ticket_has_the_shape_the_broker_requires_of_a_scoped_signer() {
        let it = issuer(&["app-feed-publish-orders", "App-Feed-Subscribe-Alarms"], 3600);
        let t = it.ticket("storm-7", 1_800_000_000).unwrap();
        let parts: Vec<&str> = t.jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(decode_part(parts[0]), br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);

        let claims: serde_json::Value = serde_json::from_slice(&decode_part(parts[1])).unwrap();
        assert_eq!(claims["iss"], it.public());
        assert_eq!(it.public(), GO_ACCOUNT_PUBLIC);
        assert_eq!(claims["iat"], 1_800_000_000u64);
        assert_eq!(claims["exp"], 1_800_003_600u64);
        assert_eq!(claims["name"], "storm-7");
        let sub = claims["sub"].as_str().unwrap();
        assert!(sub.starts_with('U') && sub.len() == 56, "subject {sub}");
        assert!(claims["jti"].as_str().unwrap().len() >= 52);
        assert!(claims.get("nbf").is_none());

        let nats = claims["nats"].as_object().unwrap();
        assert_eq!(nats["issuer_account"], GO_ACCOUNT_PUBLIC);
        assert_eq!(nats["type"], "user");
        assert_eq!(nats["version"], 2);
        // Lowercased, as the jwt library does when it adds a tag.
        assert_eq!(nats["tags"], json!(["app-feed-publish-orders", "app-feed-subscribe-alarms"]));
        // A scoped signer's JWT must have no permissions or limits: only empty pub and sub, nothing else.
        let mut keys: Vec<&String> = nats.keys().collect();
        keys.sort();
        assert_eq!(keys, ["issuer_account", "pub", "sub", "tags", "type", "version"]);
        assert_eq!(nats["pub"], json!({}));
        assert_eq!(nats["sub"], json!({}));

        // The signature is the scoped key's over "header.payload".
        let seed = decode_seed(GO_ACCOUNT_SEED, PREFIX_BYTE_ACCOUNT).unwrap();
        let pubkey = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap().public_key().as_ref().to_vec();
        let signed = format!("{}.{}", parts[0], parts[1]);
        UnparsedPublicKey::new(&ED25519, &pubkey).verify(signed.as_bytes(), &decode_part(parts[2])).unwrap();
    }

    #[test]
    fn the_nonce_is_signed_with_the_tickets_own_user_key() {
        let it = issuer(&["app-feed-publish-orders"], 3600);
        let t = it.ticket("storm-0", 1_800_000_000).unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&decode_part(t.jwt.split('.').nth(1).unwrap())).unwrap();
        let sub = claims["sub"].as_str().unwrap();
        // The user's public key, decoded from the subject (prefix, 32 bytes, checksum).
        let raw = BASE32_NOPAD.decode(sub.as_bytes()).unwrap();
        assert_eq!(raw[0], PREFIX_BYTE_USER);
        assert_eq!(crc16(&raw[..33]).to_le_bytes(), [raw[33], raw[34]]);
        let sig = t.sign_nonce("nonce-from-info");
        UnparsedPublicKey::new(&ED25519, &raw[1..33]).verify(b"nonce-from-info", &decode_part(&sig)).unwrap();
        assert!(!sig.contains('=') && !sig.contains('+') && !sig.contains('/'), "base64url without padding");
    }

    #[test]
    fn every_ticket_has_its_own_user_key() {
        let it = issuer(&["app-feed-publish-orders"], 60);
        let subject = |t: &Ticket| {
            let c: serde_json::Value = serde_json::from_slice(&decode_part(t.jwt.split('.').nth(1).unwrap())).unwrap();
            c["sub"].as_str().unwrap().to_string()
        };
        let (a, b) = (it.ticket("a", 1).unwrap(), it.ticket("b", 1).unwrap());
        assert_ne!(subject(&a), subject(&b));
        assert_ne!(a.jwt, b.jwt);
    }

    #[test]
    fn the_issuer_validates_its_inputs() {
        let roles = vec!["app-feed-publish-orders".to_string()];
        assert!(Issuer::new(GO_USER_SEED, GO_ACCOUNT_PUBLIC, &roles, 60).err().unwrap().contains("scoped seed"));
        assert!(Issuer::new("junk", GO_ACCOUNT_PUBLIC, &roles, 60).is_err());
        assert!(Issuer::new(GO_ACCOUNT_SEED, GO_USER_PUBLIC, &roles, 60).err().unwrap().contains("AUTH account public key"));
        assert!(Issuer::new(GO_ACCOUNT_SEED, GO_ACCOUNT_PUBLIC, &roles, 0).err().unwrap().contains("lifetime"));
    }

    // Tickets are made before each run's clock starts, so this is not measured, but it should stay small next to the pause between runs.
    #[test]
    fn making_sixteen_hundred_tickets_is_quick() {
        let it = issuer(&["app-feed-publish-orders", "app-feed-subscribe-alarms"], 3600);
        let start = std::time::Instant::now();
        for i in 0..1600 {
            it.ticket(&format!("storm-{i}"), 1_800_000_000).unwrap();
        }
        let took = start.elapsed();
        println!("1600 tickets in {took:?}");
        assert!(took < std::time::Duration::from_secs(5), "took {took:?}");
    }

    #[test]
    fn blank_roles_are_dropped() {
        let it = issuer(&["  ", "", "app-feed-publish-orders "], 60);
        let t = it.ticket("x", 1).unwrap();
        let c: serde_json::Value = serde_json::from_slice(&decode_part(t.jwt.split('.').nth(1).unwrap())).unwrap();
        assert_eq!(c["nats"]["tags"], json!(["app-feed-publish-orders"]));
    }
}
