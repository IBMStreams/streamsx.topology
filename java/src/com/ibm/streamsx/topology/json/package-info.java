/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
/**
 * JSON streams.
 * JSON (<a href="http://www.json.org/">JavaScript Object Notation</a>)
 * is a lightweight data-interchange format that can be used to exchange
 * streams between applications implemented in different languages.
 * <h3>Java</h3>
 * Within Java the recommended stream type for JSON data
 * is {@code TStream<JSONObject>}.
 * <BR>
 * When a tuple is a JSON array
 * the approach is to use an {@code JSONObject} with
 * a single attribute {@link com.ibm.streamsx.topology.json.JSONStreams#PAYLOAD payload}
 * containing the value (for example, an array of JSON objects}.
 * <h3>SPL</h3>
 * Within SPL the recommended stream type for JSON data
 * is {@code tuple<rstring jsonString>}, representing
 * serialized JSON.
 * (see {@link com.ibm.streamsx.topology.json.JSONSchemas#JSON JSON}).
 * This is the convention used by the SPL toolkits
 * {@code com.ibm.streamsx.json} and {@code com.ibm.streamsx.inet}.
 * <h3>Conversions</h3>
 * <TABLE border="1" style="width:50%">
 * <TR><TH colspan="2"></TH><TH colspan="2">To</TH></TR>
 * <TR><TH colspan="2">From</TH><TH>Java<BR>(TStream&lt;JSONObject>)</TH><TH>SPL<BR>({@code rstring jsonString})</TH></TR>
 * <TR><TH rowspan="3">Java</TH><TD>TStream&lt;JSONObject></TD>
 * <TD></TD><TD>{@link com.ibm.streamsx.topology.json.JSONStreams#toSPL(com.ibm.streamsx.topology.TStream) JSONStreams.toSPL()}</TD></TR>
 * <TR><TD>TStream&lt;? extends JSONAble></TD>
 * <TD>{@link com.ibm.streamsx.topology.json.JSONStreams#toJSON(com.ibm.streamsx.topology.TStream) JSONStreams.toJSON()}</TD>
 * <TD></TD></TR>
 * <TR><TD>TStream&lt;String></TD>
 * <TD>{@link com.ibm.streamsx.topology.json.JSONStreams#deserialize(com.ibm.streamsx.topology.TStream) JSONStreams.deserialize()}</TD>
 * <TD></TD></TR>
 * <TR><TH rowspan="2">SPL</TH><TD>SPLStream ({@code rstring jsonString})</TD>
 * <TD rowspan="2">{@link com.ibm.streamsx.topology.spl.SPLStream#toJSON() SPLStream.toJSON()} </TD><TD></TD>
 * </TR>
 * <TR><TD>SPLStream (any other schema)</TD><TD></TD></TR>
 * </TABLE>
 * <BR>
 * @see <a href="http://www.json.org/">http://www.json.org - JSON (JavaScript Object Notation) is a lightweight data-interchange format.</a> 
 */
package com.ibm.streamsx.topology.json;

