# SPDX-License-Identifier: Apache-2.0
import json

from stormbench import k8s


def test_json_stream_reads_one_object_a_list_or_several_in_a_row():
    one, two = {"kind": "Job", "metadata": {"name": "j"}}, {"kind": "Service", "metadata": {"name": "s"}}
    assert k8s.json_stream(json.dumps(one)) == [one]
    assert k8s.json_stream(json.dumps({"kind": "List", "items": [one, two]})) == [one, two]
    assert k8s.json_stream(json.dumps(one, indent=2) + "\n" + json.dumps(two, indent=2) + "\n") == [one, two]
    assert k8s.json_stream("") == []
