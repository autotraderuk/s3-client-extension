package uk.co.autotrader.s3clientextension

import java.net.URI

import org.scalatest.FunSpec


class S3PathSpec extends FunSpec {

  describe("When created with bucket and path") {
    val s3Path = S3Path("foo", "bar")
    it("toString returns expected") {
      assert(s3Path.toString == "s3://foo/bar")
    }
    it("toUri returns expected") {
      assert(s3Path.toUri == new URI("s3://foo/bar"))
    }
  }

  describe("When created with /path") {
    val s3Path = S3Path("foo", "/bar")
    it("toString returns expected") {
      assert(s3Path.toString == "s3://foo/bar")
    }
    it("toUri returns expected") {
      assert(s3Path.toUri == new URI("s3://foo/bar"))
    }
  }

  describe("When created with a protocol") {
    val s3Path = S3Path("foo", "/bar", protocol = "s3n")
    it("toString returns expected") {
      assert(s3Path.toString == "s3n://foo/bar")
    }
    it("toUri returns expected") {
      assert(s3Path.toUri == new URI("s3n://foo/bar"))
    }
  }

}
