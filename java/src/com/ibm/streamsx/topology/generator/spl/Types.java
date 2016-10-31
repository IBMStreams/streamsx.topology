package com.ibm.streamsx.topology.generator.spl;

class Types {
    
    /**
     * Convert the JSON representation of an SPL MetaType
     * (com.ibm.streams.operator.Type.MetaType)
     * from the Java Operator API to a SPL language type.
     */
    static String metaTypeToSPL(String metaType) {
        switch (metaType) {
        case "BOOLEAN": return "boolean";
        case "INT8": return "int8";
        case "INT16": return "int16";
        case "INT32": return "int32";
        case "INT64": return "int64";
        case "UINT8": return "uint8";
        case "UINT16": return "uint16";
        case "UINT32": return "uint32";
        case "UINT64": return "uint64";
        case "FLOAT32": return "float32";
        case "FLOAT64": return "float64";
        case "DECIMAL32": return "decimal32";
        case "DECIMAL64": return "decimal64";
        case "DECIMAL128": return "decimal128";
        case "COMPLEX32": return "complex32";
        case "COMPLEX64": return "complex64";
        case "TIMESTAMP": return "timestamp";
        case "RSTRING": return "rstring";
        case "USTRING": return "ustring";
        case "BLOB": return "blob";
        case "XML": return "xml";
        default:
            throw new UnsupportedOperationException(metaType);
        }
    }
}
