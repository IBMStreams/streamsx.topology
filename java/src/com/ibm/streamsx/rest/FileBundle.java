/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018
*/
package com.ibm.streamsx.rest;

import java.io.File;

final class FileBundle extends ApplicationBundle {
	private final File bundle;
	
	FileBundle(Instance instance, File bundle) {
		this.bundle = bundle;
		setInstance(instance);
	}
	
	File bundleFile() { return bundle;}
	
    @Override
    public void refresh() {}
}
