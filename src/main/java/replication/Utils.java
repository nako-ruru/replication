package replication;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.compressors.lzma.LZMACompressorInputStream;
import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

class Utils {

    private Utils() {
        throw new Error("死心吧，你得不到我！");
    }

    public static boolean isBlank(String s) {
        return s == null || s.isEmpty() || s.trim().isEmpty();
    }
    public static boolean isNotBlank(String s) {
        return !isBlank(s);
    }
    public static <T extends CharSequence> T defaultIfBlank(final T str, final T defaultStr) {
        return StringUtils.defaultIfBlank(str, defaultStr);
    }
    public static String abbreviate(final String str, final int maxWidth) {
        return StringUtils.abbreviate(str, maxWidth);
    }

    public static byte[] toByteArray(InputStream in) throws IOException {
        return ByteStreams.toByteArray(in);
    }
    public static void copy(InputStream in, OutputStream out) throws IOException {
        ByteStreams.copy(in, out);
    }

    public static <K, V> Map<K, V> map(Object... kvPairs) {
        Map<K, V> map = new LinkedHashMap<>();
        for(int i = 0; i < kvPairs.length; i += 2){
            map.put((K) kvPairs[i], (V) kvPairs[i + 1]);
        }
        return map;
    }

    public static String toJson(Object o) {
        try {
            return newObjectMapper().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
    public static <T> T fromJson(String json, Class<T> classOfT) {
        try {
            return newObjectMapper().readValue(json, classOfT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] compress(byte[] input) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(input);
             ByteArrayOutputStream baos = new ByteArrayOutputStream(input.length / 20);
             LZMACompressorOutputStream lzmacos = new LZMACompressorOutputStream(baos)) {
            copy(bais, lzmacos);
            lzmacos.finish();
            return baos.toByteArray();
        }
    }
    public static byte[] decompress(byte[] input, int offset, int length) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(input, offset, length);
             InputStream lzmacis = new LZMACompressorInputStream(bais);
             ByteArrayOutputStream baos = new ByteArrayOutputStream(length)) {
            copy(lzmacis, baos);
            return baos.toByteArray();
        }
    }

    private static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        return mapper;
    }
}

