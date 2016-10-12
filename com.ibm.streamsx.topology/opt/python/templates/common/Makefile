all:
	chmod 500 com.ibm.streamsx.topology/com.ibm.streamsx.topology.functional.python/pyversion.sh
	chmod 500 com.ibm.streamsx.topology/com.ibm.streamsx.topology.functional.python2/pyversion.sh
	@for toolkit in `cat manifest_tk.txt`; do \
		${STREAMS_INSTALL}/bin/spl-make-toolkit -i $${toolkit}; \
	done;
	${STREAMS_INSTALL}/bin/sc -M `cat main_composite.txt` --rebuild-toolkits --no-toolkit-indexing -t ${STREAMS_INSTALL}/toolkits
