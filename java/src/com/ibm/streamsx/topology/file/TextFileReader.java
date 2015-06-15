/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.types.RString;

@PrimitiveOperator
@InputPortSet(cardinality = 1)
@OutputPortSet(cardinality = 1)
public class TextFileReader extends AbstractOperator {

    private String encoding = "UTF-8";
    private Charset charset;

    public String getEncoding() {
        return encoding;
    }

    @Parameter(optional = true)
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    @Override
    public void initialize(OperatorContext context) throws Exception {
        super.initialize(context);

        charset = Charset.forName(getEncoding());
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {

        final StreamingOutput<OutputTuple> out = getOutput(0);

        String fileName = tuple.getString(0);

        File file = new File(fileName);

        if (!file.isAbsolute()) {
            file = new File(getOperatorContext().getPE().getDataDirectory(),
                    fileName);
        }

        FileInputStream fis = new FileInputStream(file);
        try {

            BufferedReader br = new BufferedReader(new InputStreamReader(fis,
                    charset), 128 * 1024);

            for (;;) {
                String line = br.readLine();
                if (line == null)
                    break;
                out.submitAsTuple(new RString(line));
            }
            br.close();

        } finally {
            fis.close();
        }
    }
}
