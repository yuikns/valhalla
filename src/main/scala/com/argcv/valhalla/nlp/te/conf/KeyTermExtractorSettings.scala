package com.argcv.valhalla.nlp.te.conf

/**
 * some magic numbers for key term extractor.
 * based on source: stdafx.h
 *
 * @author yu
 */
trait KeyTermExtractorSettings {
  /**
   * the magic numbers here
   * the average sentence length in words
   */
  lazy val SENTENCE_WORDS: Int = 50

  /**
   * Name these definitions so long to reduce the chance of macro redifinition
   * The definition for max line length in WORDS.
   */
  lazy val EXTRACTION_MAXWORDNUM_PER_LINE: Int = 100

  /**
   * average word length
   */
  lazy val EXTRACTION_AVERAGE_WORDLENGTH: Int = 5

  /**
   * threshold: max word number per term
   */
  lazy val EXTRACTION_MAX_WORDS_PER_TERM: Int = 4
}

object KeyTermExtractorSettings extends KeyTermExtractorSettings
