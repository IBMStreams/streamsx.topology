.PHONY: all clean 

TOPOLOGY_VERSION?=1.16.0
LIB_DIR=extlib
BIN_LOC=classes
DOWNLOAD_DIR=download
JSI = $(STREAMS_INSTALL)


all: runtopo

javacompile: $(BIN_LOC)
	javac -g -classpath $(LIB_DIR)/streamsx.topology-$(TOPOLOGY_VERSION).jar:$(LIB_DIR)/gson-2.3.1.jar:$(JSI)/lib/com.ibm.streams.operator.samples.jar -d $(BIN_LOC) src/topo1/App.java
	${STREAMS_INSTALL}/bin/spl-make-toolkit -i .

$(BIN_LOC):
	mkdir -p $(BIN_LOC)

mvndownload: $(LIB_DIR)
	export TOPOLOGY_VERSION=$(TOPOLOGY_VERSION); mvn -f pom.xml --batch-mode dependency:copy-dependencies -DoutputDirectory=$(LIB_DIR)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

downloadlatest: $(DOWNLOAD_DIR)
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -Dartifact=com.ibm.streams:streamsx.topology:LATEST:jar
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -Dartifact=com.ibm.streams:streamsx.topology:LATEST:jar:sources
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -Dartifact=com.ibm.streams:streamsx.topology:LATEST:jar:javadoc
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -Dartifact=com.ibm.streams:streamsx.topology:LATEST:tgz:toolkit

downloadstaging: $(DOWNLOAD_DIR)
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -DrepoUrl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -Dartifact=com.ibm.streams:streamsx.topology:$(TOPOLOGY_VERSION):jar
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -DrepoUrl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -Dartifact=com.ibm.streams:streamsx.topology:$(TOPOLOGY_VERSION):jar:sources
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -DrepoUrl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -Dartifact=com.ibm.streams:streamsx.topology:$(TOPOLOGY_VERSION):jar:javadoc
	mvn --batch-mode dependency:copy -DoutputDirectory=$(DOWNLOAD_DIR) -DrepoUrl=https://oss.sonatype.org/service/local/staging/deploy/maven2/ -Dartifact=com.ibm.streams:streamsx.topology:$(TOPOLOGY_VERSION):tgz:toolkit

$(DOWNLOAD_DIR):
	mkdir -p $(DOWNLOAD_DIR)

runtopo: mvndownload javacompile
	java -classpath $(BIN_LOC):$(LIB_DIR)/streamsx.topology-$(TOPOLOGY_VERSION).jar:$(LIB_DIR)/gson-2.3.1.jar:$(JSI)/lib/com.ibm.streams.operator.samples.jar topo1.App

run: runtopo
	java -jar topo1.temperatureSensor.sab

clean: 
	rm -rf $(BIN_LOC)
	rm -rf $(LIB_DIR)
	rm -rf $(DOWNLOAD_DIR)
	rm -f topo1*.sab
	rm -f topo1*.json
	rm -rf output
	rm -rf toolkit.xml
