package com.linkedin.metadata.queue.postgres;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class PgQueueContentTypeRegistrationTest {

  @Test
  public void ensureContentTypeLookupBeforeInsert() {
    String lookup = "SELECT id FROM queue.metadata_queue_content_type WHERE mime = ?";
    String insert = "INSERT INTO queue.metadata_queue_content_type (mime) VALUES (?)";
    assertTrue(lookup.contains("WHERE mime = ?"));
    assertTrue(!insert.contains("ON CONFLICT"), "conflict upserts burn smallint identity");
  }
}
