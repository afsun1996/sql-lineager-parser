package com.afsun.lineage.core.exceptions;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.helpers.MessageFormatter;

/**
 * @author afsun
 * @date 2025-11-03日 11:16
 */
public class MetadataNotFoundException extends RuntimeException{
    public MetadataNotFoundException(String message, Object... args) {
        super(MessageFormatter.arrayFormat(message, trimLastThrowable(args)).getMessage(), extractThrowable(args));
    }

    public MetadataNotFoundException(String message){
        super(message);
    }

    private static Throwable extractThrowable(Object[] args) {
        if (ArrayUtils.isEmpty(args)) {
            return null;
        }

        Object last = args[args.length - 1];
        if (last instanceof Throwable) {
            return (Throwable) last;
        }
        return null;
    }

    public static Object[] trimLastThrowable(Object[] argumentArray) {
        if (ArrayUtils.isEmpty(argumentArray)) {
            return argumentArray;
        }
        if (extractThrowable(argumentArray) == null) {
            return argumentArray;
        }
        int trimmedLen = argumentArray.length - 1;
        if (trimmedLen == 0) {
            return new Object[0];
        }
        Object[] trimmedArray = new Object[trimmedLen];
        System.arraycopy(argumentArray, 0, trimmedArray, 0, trimmedLen);
        return trimmedArray;
    }
}
