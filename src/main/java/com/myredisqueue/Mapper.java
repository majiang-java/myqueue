package com.myredisqueue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import com.sun.xml.internal.ws.encoding.soap.SerializationException;

import java.io.IOException;

public class Mapper<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private JavaType tClass;
    public  Mapper(JavaType tClass){
        this.tClass = tClass;
    }

    public Mapper(Class<T> tClass) {
        this.tClass = objectMapper.getTypeFactory().constructType(tClass);
    }

    public String serialize(T element){
        Class<?> aClass = element.getClass();
        if(!objectMapper.canSerialize(aClass)) {
            throw new SerializationException("Could not serialize class " + aClass.getName(), element);
        }else {
            try{
                return objectMapper.writeValueAsString(element);
            }catch (JsonProcessingException e){
                throw new SerializationException("Error while trying to serialize element", element, e);
            }
        }
    }

    public T deserialize(String element) {
        try {
            return objectMapper.readValue(element, tClass);
        }catch (IOException e) {
            throw new DeserializationException("Could not deserialize string", element, e);
        }
    }
}
