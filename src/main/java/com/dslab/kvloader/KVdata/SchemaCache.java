/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.KVdata;

import java.util.Iterator;
import java.util.List;

/**
 *
 * @author hansone123
 */
public class SchemaCache {
    private final List<TableSchema> schemas;
    public SchemaCache(final List<TableSchema> schemas) {
        this.schemas = schemas;
    }
    public void append(TableSchema schema) {
        schemas.add(schema);
    }
    
    public Iterator<TableSchema> iterator() {
        return this.schemas.iterator();
    }
    public int size() {
        return schemas.size();
    }
    
}
