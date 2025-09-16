./gradlew cleanEclipse eclipse # force to download sources
./gradlew cleanEclipse # cleanup eclipse files
./gradlew clean assemble testClasses
./gradlew --console=plain --init-script init-script-bloop.gradle bloopInstall
bloop projects