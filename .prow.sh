#! /bin/bas
hexport CSI_PROW_KUBERNETES_VERSION="1.34.0"
export CSI_PROW_KUBERNETES_DEPLOYMENT="1.34"
export CSI_PROW_E2E_VERSION="release-1.34"
export CSI_PROW_DEPLOYMENT_SUFFIX=""
export CSI_PROW_DRIVER_VERSION="v1.17.0"
export CSI_SNAPSHOTTER_VERSION="v6.1.0"
export CSI_PROW_TESTS="sanity serial parallel"
export CSI_PROW_BUILD_PLATFORMS="linux amd64 amd64"
export CSI_PROW_E2E_FOCUS_1_34="\[FeatureGate:VolumeAttributesClass\]"

custom_post_install() {
    echo "Running custom post install hook..."
}

export CSI_PROW_DRIVER_INSTALL="custom_post_install"
. release-tools/prow.sh
main

