package com.datastax.driver.core.schemabuilder;

import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;


public class CompressionOptionsTest {

    @Test
    public void should_build_compressions_options_for_lz4() throws Exception {
        //When
        final String build = TableOptions.CompressionOptions.lz4().withChunkLengthInKb(128).withCRCCheckChance(0.6D).build();

        //Then
        assertThat(build).isEqualTo("{'sstable_compression' : 'LZ4Compressor', 'chunk_length_kb' : 128, 'crc_check_chance' : 0.6}");
    }

    @Test
    public void should_create_snappy_compressions_options() throws Exception {
        //When
        final String build = TableOptions.CompressionOptions.snappy().withChunkLengthInKb(128).withCRCCheckChance(0.6D).build();

        //Then
        assertThat(build).isEqualTo("{'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : 128, 'crc_check_chance' : 0.6}");
    }

    @Test
    public void should_create_deflate_compressions_options() throws Exception {
        //When
        final String build = TableOptions.CompressionOptions.deflate().withChunkLengthInKb(128).withCRCCheckChance(0.6D).build();

        //Then
        assertThat(build).isEqualTo("{'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 128, 'crc_check_chance' : 0.6}");
    }

    @Test
    public void should_create_no_compressions_options() throws Exception {
        //When
        final String build = TableOptions.CompressionOptions.none().withChunkLengthInKb(128).withCRCCheckChance(0.6D).build();

        //Then
        assertThat(build).isEqualTo("{'sstable_compression' : ''}");
    }
}
