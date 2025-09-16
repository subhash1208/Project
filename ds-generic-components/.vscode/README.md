# Local VS Code setup

Install bloop

```sh
brew install scalacenter/bloop/bloop
brew services start scalacenter/bloop/bloop

# Add mirror to coursier
printf "central.from=https://repo1.maven.org/maven2\ncentral.to=https://artifactory.us.caas.oneadp.com/artifactory/central-maven-remote\n" > ~/Library/Preferences/Coursier/mirror.properties

# download jars using coursier from cli directly (not need if the mirror is setproperly)
cs fetch --no-default \
  -r https://artifactory.us.caas.oneadp.com/artifactory/central-maven-remote \
  ch.epfl.scala:bloop-frontend_2.12:1.4.9


# Not related, but may be useful elsewhere

# Download proxy.pem file
keytool -J-Dhttps.proxyHost=usproxy.es.oneadp.com -J-Dhttps.proxyPort=8080 -printcert -rfc -sslserver repo1.maven.org:443

# Copy the last certificate from the above file into usproxy.pem file

# Add it to local cacerts file
cd $JAVA_HOME/jre/lib/security
keytool -keystore cacerts -import -file usproxy.pem -alias usproxy


```