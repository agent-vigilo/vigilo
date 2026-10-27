FROM quay.io/ceph/ceph:v20.2.4

# The SDK is only for local S3 bootstrap, readiness, and infrastructure tests.
# Keep Ceph's system Python unchanged; current Boto3 needs Python >= 3.10.
RUN dnf install -y python3.12-pip && dnf clean all
COPY requirements.txt /opt/vigilo-ceph-requirements.txt
RUN python3.12 -m pip install --no-cache-dir -r /opt/vigilo-ceph-requirements.txt
