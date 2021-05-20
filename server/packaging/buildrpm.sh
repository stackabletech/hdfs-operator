#!/usr/bin/env bash
# This script creates an RPM package containing the binary created by this Cargo project.
# The script is not universally applicable, since it makes a few assumptions about the project structure:
#  1. The RPM scaffolding needs to be provided in server/packaging/rpm
#  2. The binary to be packaged needs to be created in target/release

# The script takes one argument, which is the name of the binary that has been created by the build process.
# This argument will be reused for naming the final RPM file.

# Check if one parameter was specified - we'll use this as the name parameter for all files
# This allows us to reuse the script across all operators
if [ -z $1 ]; then
  echo "This script requires the project name to be specified as the first parameter!"
  exit 1
fi

export PACKAGE_NAME=$1
BINARY_FILE=target/release/$PACKAGE_NAME

# The package description is parsed from the output of `cargo metadata` by using jq.
# We need to look up the package with a select statement to match the name from an array of packages
# The name is passed into jq as a jq variable, as no substitution would take place within the single
# quotes of the jq expression.
export PACKAGE_DESCRIPTION=$(~/.cargo/bin/cargo metadata --format-version 1| jq --arg NAME "$PACKAGE_NAME" '.packages[] | select(.name == $NAME) | .description')
if [ -z $PACKAGE_DESCRIPTION ]; then
  echo "Unable to parse package description from output of `cargo metadata`, cannot build RPM without this field!"
  exit 2
fi
echo

# Check that we are being called from the main directory and the release build process has been run
if [ ! -f $BINARY_FILE ]; then
    echo "Binary file not found at [$BINARY_FILE] - this script should be called from the root directory of the repository and 'cargo build --release' needs to have run before calling this script!"
    exit 3
fi

echo Cleaning up prior build attempts
rm -rf target/rpm

# Parse the version and release strings from the PKGID reported by Cargo
# This is in the form Path#Projectname:version, which we parse by repeated calls to awk with different separators
# This could most definitely be improved, but works for now
export VERSION_STRING=$(~/.cargo/bin/cargo pkgid --manifest-path server/Cargo.toml  | awk -F'#' '{print $2}' |  awk -F':' '{print $2}')
echo version: ${VERSION_STRING}

export PACKAGE_VERSION=$(echo ${VERSION_STRING} | awk -F '-' '{print $1}')

# Any suffix like '-nightly' is split out into the release here, as - is not an allowed character in rpm versions
# The final release will look like 0.suffix or 0 if no suffix is specified.
export PACKAGE_RELEASE="0$(echo ${VERSION_STRING} | awk -F '-' '{ if ($2 != "") print "."$2;}')"

echo Defined package version: [${PACKAGE_VERSION}]
echo Defined package release: [${PACKAGE_RELEASE}]
echo Defined package description: [${PACKAGE_DESCRIPTION}]


echo Creating directory scaffolding for RPM
cp -r server/packaging/rpm target/
# Create empty directory for the binary to be placed into
mkdir -p target/rpm/SOURCES/${PACKAGE_NAME}-VERSION/opt/stackable/${PACKAGE_NAME}

# The packaging source directory does not contain the version yet, as this will need to be replaced for every
# execution. Instead the directory name contains the marker "VERSION" which we now replace with the actual version.
rename VERSION ${PACKAGE_VERSION} target/rpm/SOURCES/${PACKAGE_NAME}-VERSION

cp target/release/${PACKAGE_NAME} target/rpm/SOURCES/${PACKAGE_NAME}-${PACKAGE_VERSION}/opt/stackable/${PACKAGE_NAME}/

pushd target/rpm/SOURCES
tar czvf ${PACKAGE_NAME}-${PACKAGE_VERSION}.tar.gz ${PACKAGE_NAME}-${PACKAGE_VERSION}
popd

rpmbuild --define "_topdir `pwd`/target/rpm" -v -ba target/rpm/SPECS/${PACKAGE_NAME}.spec
