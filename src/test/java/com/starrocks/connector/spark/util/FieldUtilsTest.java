package com.starrocks.connector.spark.util;

import org.junit.jupiter.api.Test;

import static com.starrocks.connector.spark.util.FieldUtils.eraseQuote;
import static com.starrocks.connector.spark.util.FieldUtils.quoteIfNecessary;
import static org.junit.jupiter.api.Assertions.*;

class FieldUtilsTest {

    @Test
    void quoteTest() {
        assertEquals("``", quoteIfNecessary(""));
        assertEquals("```", quoteIfNecessary("`"));
        assertEquals("``", quoteIfNecessary("``"));
        assertEquals("`aaa`", quoteIfNecessary("aaa"));
        assertEquals("`aaa`", quoteIfNecessary("`aaa`"));
        assertEquals("` aaa`", quoteIfNecessary("` aaa`"));
        assertEquals("`` aaa `", quoteIfNecessary("` aaa "));
        assertEquals("`aaa ``", quoteIfNecessary("aaa `"));
    }

    @Test
    void eraseQuoteTest() {
        assertEquals("", eraseQuote(""));
        assertEquals("`", eraseQuote("`"));
        assertEquals("", eraseQuote("``"));
        assertEquals("aaa", eraseQuote("aaa"));
        assertEquals("aaa", eraseQuote("`aaa`"));
        assertEquals(" aaa", eraseQuote("` aaa`"));
        assertEquals("` aaa ", eraseQuote("` aaa "));
        assertEquals("aaa `", eraseQuote("aaa `"));
    }

}