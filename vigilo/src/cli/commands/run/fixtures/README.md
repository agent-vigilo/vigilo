# Dataset identity compatibility fixtures

`dataset-identity.json` and `dataset-identity.yaml` describe equal parsed values
with different source formatting and key ordering. They cover nested objects and
arrays, UTF-8, a fractional number, nulls, omitted optional fields, duplicate tags,
metadata, explicit routing, and equal case content under distinct case IDs.

`dataset-identity.expected.json` freezes exact canonical JSON bytes, case hashes,
ordered membership, dataset hash, UUID preimage, and dataset-version UUID. These
values were calculated outside Vigilo with Python's standard JSON library
(`sort_keys=True`, `ensure_ascii=False`, `separators=(',', ':')`) and `b3sum` on
UTF-8 bytes without a trailing newline. The UUID uses the first 16 digest bytes,
with byte 6 masked/set to `(byte & 0x0f) | 0x80` and byte 8 to
`(byte & 0x3f) | 0x80`. These tools are not needed to run the tests.

The tests compare the existing CLI implementation to these fixed values, then
exercise identity-preserving and identity-changing variations. Do not regenerate
the oracle from implementation output. An intentional identity change requires
an explicit scheme and compatibility decision, not updated snapshots alone.

Run with:

```bash
cargo test -p vigilo --locked --bin vigilo identity_tests
```

This freezes existing observable identity behavior. It does not lock unsupported
input acceptance or prescribe the future batch/manifest wire format. See the
[blob storage contract](https://agentvigilo.com/docs/architecture/case-storage/).
