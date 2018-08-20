package uk.co.autotrader.s3clientextension

import java.net.URI

/**
  * S3 Path for AmazonS3.
  * @param bucketName The name of the Amazon S3 bucket.
  * @param objectPath The object path.
  * @param protocol The Amazon S3 protocol e.g. "s3". Defaults to "s3" if not supplied.
  *
  */
case class S3Path(bucketName: String, objectPath: String, protocol: String = "s3") {

  /**
    * @return A string representation of the S3Path object.
    */
  override def toString: String = s"$protocol://$bucketName/${ltrim(objectPath)}"

  /**
    * @return A URI representation of the S3Path object.
    */
  def toUri: URI = new URI(this.toString)

  private def ltrim(s: String) = s.replaceAll("^/+", "")

}
