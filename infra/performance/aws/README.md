# AWS Performance Host

`template.yaml` provisions the ephemeral EC2 host for the canonical
`aws-m6i-2xlarge-al2023-v1` performance environment. It provisions the host, installs service prerequisites,
and checks its declared environment identity before measurements.

The template is the provisioning implementation for
`performance/environments/aws-m6i-2xlarge-al2023-v1.toml`. The TOML remains the
measurement identity consumed by the harness.

## Frozen Contract

| Property | Value |
| --- | --- |
| Region | `us-west-2` |
| Availability Zone ID | `usw2-az1` |
| AMI | `ami-0c5f78ca5e1169a1a` (`al2023-ami-2023.9.20251117.1-kernel-6.12-x86_64`) |
| Instance | On-demand `m6i.2xlarge`, x86_64, 8 vCPUs, 32 GiB |
| Storage | Encrypted 100 GiB `gp3`, 6,000 IOPS, 250 MiB/s, delete on termination |
| Access | Systems Manager Session Manager; no inbound security-group rules |
| Lifetime | Guest shutdown terminates the instance; a 12-hour timer bounds unattended hosts |

Changing any performance-relevant value requires a new environment ID, fresh
noise calibration, and new budgets. Do not replace the AMI with an SSM
"latest" parameter under the existing ID.

## Deploy

The AWS identity deploying the stack needs permission to manage CloudFormation,
EC2, IAM roles and instance profiles. It also needs `iam:PassRole`. Validate and
deploy from the repository root:

```bash
aws cloudformation validate-template \
  --region us-west-2 \
  --template-body file://infra/performance/aws/template.yaml

aws cloudformation deploy \
  --region us-west-2 \
  --stack-name vigilo-performance \
  --template-file infra/performance/aws/template.yaml \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset
```

Get the instance ID and open an SSM session:

```bash
INSTANCE_ID=$(aws cloudformation describe-stacks \
  --region us-west-2 \
  --stack-name vigilo-performance \
  --query "Stacks[0].Outputs[?OutputKey=='InstanceId'].OutputValue" \
  --output text)
aws ssm start-session --region us-west-2 --target "$INSTANCE_ID"
```

Stack creation waits up to 30 minutes for bootstrap and the first host
attestation. A bootstrap error or timeout fails stack creation. In the session,
rerun the fail-closed attestation immediately before a campaign:

```bash
sudo /usr/local/bin/vigilo-performance-attest \
  | tee /opt/vigilo-performance/host-attestation.json
```

The bootstrap installs Docker, a checksum-pinned Docker Compose plugin, Git, and
the native build prerequisites. It disables swap and transparent huge pages,
applies the available guest CPU controls, and exports
`VIGILO_PERF_ENVIRONMENT_ID`. It deliberately does not set
`VIGILO_PERF_CANONICAL_VALIDATED`; external certification remains required.

Install and register the GitHub Actions runner as `ec2-user` with the labels
`self-hosted`, `linux`, `x64`, and `vigilo-performance`, following GitHub's
self-hosted runner instructions. Use an ephemeral runner registration for a
one-campaign host. Do not run unrelated workloads on the instance.

## Teardown

Delete the stack after artifacts have been uploaded:

```bash
aws cloudformation delete-stack \
  --region us-west-2 \
  --stack-name vigilo-performance
aws cloudformation wait stack-delete-complete \
  --region us-west-2 \
  --stack-name vigilo-performance
```

The instance also terminates when the guest shuts down or the 12-hour timer
expires. Delete the CloudFormation stack afterward to remove its VPC, subnet,
route table, security group, and IAM resources.
