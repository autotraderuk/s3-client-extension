package uk.co.autotrader.s3clientextension

import java.io.File
import java.util.{List => JavaList}

import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import scala.collection.JavaConverters._
import scala.collection.parallel.ParSeq


/**
  * S3 Client for AmazonS3.
  * @param amazonS3 AmazonS3 object.
  *
  */
class S3Client(amazonS3: AmazonS3) {

  /**
    * Gets a fully paginated list of input streams containing the contents of the objects in the specified bucket.
    *
    * @param bucket The name of the Amazon S3 bucket to list.
    * @param prefix Restricts the response to keys beginning with the specified prefix.
    * @param fileTypeFilter Optional param to filter object content by fileType.
    * e.g. Some(".avro") would return Avro object summaries only. Defaults to None if not supplied.
    * @return A fully paginated Sequence of S3ObjectInputStream.
    */
  def getAllObjectContent(bucket: String,
                          prefix: String,
                          fileTypeFilter: Option[String] = None): Seq[S3ObjectInputStream] = {
    getAllObjectSummaries(bucket, prefix, fileTypeFilter)
      .view
      .map(s => amazonS3.getObject(s.getBucketName, s.getKey).getObjectContent)
  }

  /**
    * Gets a fully paginated list of object summaries describing the objects in the specified bucket.
    *
    * @param bucket The name of the Amazon S3 bucket to list.
    * @param prefix Restricts the response to keys beginning with the specified prefix.
    * @param fileTypeFilter Optional param to filter object summaries by fileType.
    * e.g. "Some(".avro") would return Avro object summaries only. Defaults to None if not supplied.
    * @return A fully paginated Sequence of S3ObjectSummary.
    */
  def getAllObjectSummaries(bucket: String,
                            prefix: String,
                            fileTypeFilter: Option[String] = None): Seq[S3ObjectSummary] = {
    listAllObjects(bucket, prefix)
      .flatMap(_.getObjectSummaries.asScala.toSeq)
      .filter(s => fileTypeFilter.forall(s.getKey.endsWith(_)))
  }

  /**
    * Gets a fully paginated list of object listings describing the objects in the specified bucket.
    *
    * @param bucket The name of the Amazon S3 bucket to list.
    * @param prefix Restricts the response to keys beginning with the specified prefix.
    * @return A fully paginated Sequence of ObjectListings.
    */
  def listAllObjects(bucket: String, prefix: String): Seq[ObjectListing] = {
    val stream = Stream.iterate(amazonS3.listObjects(bucket, prefix)) { amazonS3.listNextBatchOfObjects }
    val objectListings =
      stream
        .takeWhile(_.isTruncated)
        .foldLeft(Seq.empty[ObjectListing]) { (acc, element) => acc :+ element }

    objectListings :+ stream.dropWhile(_.isTruncated).head
  }

  /**
    * Uploads a file to the specified bucket and key with optional encryption.
    *
    * @param file The File object to upload.
    * @param bucket The name of an existing bucket to which the new file will be uploaded.
    * @param key The key under which to store the new file.
    * @param encrypted If true, sets the server-side encryption algorithm; when encrypting the object using AWS-managed keys.
    *                  Defaults to true if not specified.
    * @return PutObjectResult
    */
  def uploadFile(file: File,
                 bucket: String,
                 key: String,
                 encrypted: Boolean = true): PutObjectResult = {
    val objectMetadata = new ObjectMetadata
    objectMetadata.setContentLength(file.length)
    if(encrypted) objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)

    val putObjectRequest = new PutObjectRequest(bucket, key, file)
    putObjectRequest.setMetadata(objectMetadata)
    putObjectRequest.setCannedAcl(CannedAccessControlList.BucketOwnerFullControl)

    amazonS3.putObject(putObjectRequest)
  }

  /**
    * Deletes multiple objects from the specified bucket.
    *
    * @param bucket The name of the Amazon S3 bucket from which the objects will be deleted.
    * @param prefixes Restricts the response to keys beginning with the specified prefixes.
    * @return An integer representing the number of objects deleted from S3.
    */
  def deleteObjects(bucket: String, prefixes: Seq[String]): Int = {
    val keyVersions = prefixes.map(prefix => new KeyVersion(prefix)).asJava
    val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions).withQuiet(false)
    val deleteObjectsResult = amazonS3.deleteObjects(deleteObjectsRequest)
    deleteObjectsResult.getDeletedObjects.size
  }

  /**
    * Sets a tag for the objects in the specified bucket.
    *
    * @param bucket The name of an existing bucket in which the files to tag exist.
    * @param prefixes Restricts the tag addition to keys beginning with the specified prefixes.
    * @param tag The tag to add.
    */
  def addTagForPrefixes(bucket: String, prefixes: Seq[String], tag: Tag): Unit = {
    getKeys(bucket, prefixes)
      .foreach(objectKey => {
        val tagSet = getTagsForKey(bucket, objectKey).asJava
        tagSet.add(tag)
        setObjectTagging(bucket, objectKey, tagSet)
      })
  }

  /**
    * Deletes a tag for the objects in the specified bucket.
    *
    * @param bucket The name of an existing bucket in which the files to untag exist.
    * @param prefixes Restricts the tag deletion to keys beginning with the specified prefixes.
    * @param tagKey The key of the tag to delete.
    */
  def deleteTagForPrefixes(bucket: String, prefixes: Seq[String], tagKey: String): Unit = {
    getKeys(bucket, prefixes)
      .foreach(objectKey => {
        val unmatchedTags = getTagsForKey(bucket, objectKey).filter(_.getKey != tagKey).asJava
        setObjectTagging(bucket, objectKey, unmatchedTags)
      })
  }

  /**
    * Gets a fully paginated parallel list of object keys for the specified bucket.
    *
    * @param bucket The name of the Amazon S3 bucket to list.
    * @param prefixes Restricts the response to keys beginning with the specified prefixes.
    * @return A fully paginated parallel sequence of object keys as Strings.
    */
  def getKeys(bucket: String, prefixes: Seq[String]): ParSeq[String] = {
    prefixes.flatMap { prefix => getAllObjectSummaries(bucket, prefix).map(_.getKey) }.par
  }

  private def getTagsForKey(bucket: String, key: String): Seq[Tag] = {
    val getObjectTaggingRequest = new GetObjectTaggingRequest(bucket, key)
    amazonS3.getObjectTagging(getObjectTaggingRequest).getTagSet.asScala
  }

  private def setObjectTagging(bucket: String, objectKey: String, tagSet: JavaList[Tag]): SetObjectTaggingResult = {
    val setObjectTaggingRequest = new SetObjectTaggingRequest(bucket, objectKey, new ObjectTagging(tagSet))
    amazonS3.setObjectTagging(setObjectTaggingRequest)
  }

}

object S3Client {
  def apply(amazonS3: AmazonS3) = new S3Client(amazonS3)

  /**
    * @return S3Client object with a default AmazonS3 using the
    *         { @link com.amazonaws.services.s3.S3CredentialsProviderChain}
    *         and { @link com.amazonaws.regions.DefaultAwsRegionProviderChain} chain
    */
  def withDefaultAmazonS3 = new S3Client(AmazonS3ClientBuilder.defaultClient())
}
