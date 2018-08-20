package uk.co.autotrader.s3clientextension

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util
import java.util.Scanner

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{Tag => S3Tag, _}
import org.scalatest._

import scala.collection.JavaConverters._


class S3ClientSpec extends FunSpec with BeforeAndAfter with Matchers {

  private val BUCKET_NAME = "test-bucket"
  private val KEY = "object-key"
  private val amazonS3 = AmazonS3ClientBuilder
    .standard()
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("accessKey", "secretKey")))
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:4572", "us-west-1"))
    .enablePathStyleAccess()
    .build()

  private val s3Client = S3Client(amazonS3)

  after {
    s3Client.getAllObjectSummaries(BUCKET_NAME, KEY)
      .foreach { summary => amazonS3.deleteObject(BUCKET_NAME, summary.getKey) }
  }

  describe("When getting S3 objects") {

    it("should get all object listings") {
      amazonS3.createBucket(BUCKET_NAME)
      Range(1, 1100).foreach { i => amazonS3.putObject(BUCKET_NAME, s"$KEY/$i.txt", s"$i") }

      val objectListings = s3Client.listAllObjects(BUCKET_NAME, KEY)

      objectListings.size shouldBe 2
    }

    it("should get object summaries for all objects") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.avro", "test1")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/_SUCCESS", "test2")

      val objectSummaries = s3Client.getAllObjectSummaries(BUCKET_NAME, KEY)

      objectSummaries.size shouldBe 2
    }

    it("should get object keys for all objects with the prefix") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/key2/test1.avro", "test1")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/key2/_SUCCESS", "test2")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test2.txt", "test3")

      val objectKeys = s3Client.getKeys(BUCKET_NAME, List(s"$KEY/key2"))

      objectKeys.size shouldBe 2
    }

    it("should get object summaries for given file type") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.avro", "test1")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/_SUCCESS", "test2")

      val objectSummaries = s3Client.getAllObjectSummaries(BUCKET_NAME, KEY, Some("avro"))

      objectSummaries.size shouldBe 1
    }

    it("should get object content") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.avro", "test1")

      val objectContent = s3Client.getAllObjectContent(BUCKET_NAME, KEY)

      val contentAsString = convertStreamToString(objectContent.head)
      contentAsString shouldBe "test1"
    }

    it("should get no object content for an incorrect prefix") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.avro", "contents")

      val objectContent: Seq[S3ObjectInputStream] = s3Client.getAllObjectContent(BUCKET_NAME, "test")
      objectContent shouldBe Seq[S3ObjectInputStream]()
    }

  }

  describe("When uploading files into S3") {
    val testFilePath: Path = Files.createTempFile("test", "txt")
    val testFile = testFilePath.toFile
    val fileContent = "file contents"
    Files.write(testFilePath, fileContent.getBytes(StandardCharsets.UTF_8))
    testFile.deleteOnExit()

    // TODO: Find a way to test this without using real S3.
    ignore("should put an object with encryption") {
      amazonS3.createBucket(BUCKET_NAME)

      val putObjectResult = s3Client.uploadFile(testFile, BUCKET_NAME, KEY)

      putObjectResult.getSSEAlgorithm shouldBe ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION
      assertFileContent
    }

    it("should put an object without encryption") {
      amazonS3.createBucket(BUCKET_NAME)

      val putObjectResult = s3Client.uploadFile(testFile, BUCKET_NAME, KEY, encrypted = false)

      putObjectResult.getSSEAlgorithm shouldBe null
      assertFileContent
    }

    def assertFileContent: Assertion = {
      val objectSummary = amazonS3.listObjects(BUCKET_NAME, KEY).getObjectSummaries.get(0)
      val s3Object = amazonS3.getObject(objectSummary.getBucketName, objectSummary.getKey)
      val content = convertStreamToString(s3Object.getObjectContent)

      content shouldBe fileContent
    }

  }

  describe("When managing Object Tagging") {

    it("should add a tag") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, KEY, "content")

      val tag = new S3Tag("key", "value")
      s3Client.addTagForPrefixes(BUCKET_NAME, List(KEY), tag)

      getObjectTagging shouldBe List(tag).asJava
    }

    it("should delete a tag") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, KEY, "content")

      val tagSet = List(new S3Tag("key", "value"), new S3Tag("key2", "value2"))
      val setObjectTaggingRequest = new SetObjectTaggingRequest(BUCKET_NAME, KEY, new ObjectTagging(tagSet.asJava))
      amazonS3.setObjectTagging(setObjectTaggingRequest)

      s3Client.deleteTagForPrefixes(BUCKET_NAME, List(KEY), tagSet.head.getKey)

      getObjectTagging shouldBe List(tagSet.last).asJava
    }

    def getObjectTagging: util.List[S3Tag] = amazonS3.getObjectTagging(new GetObjectTaggingRequest(BUCKET_NAME, KEY)).getTagSet

  }

  describe("When deleting objects in S3") {

    it("should delete all objects") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.avro", "test1")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/_SUCCESS", "test2")

      val objectKeys = s3Client.getKeys(BUCKET_NAME, List(KEY)).seq
      val noOfDeletedObjects = s3Client.deleteObjects(BUCKET_NAME, objectKeys)

      noOfDeletedObjects shouldBe 2
    }

    it("should only delete the specified objects") {
      amazonS3.createBucket(BUCKET_NAME)
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test1.txt", s"test1")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/key2/test2.txt", s"test2")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test3.txt", s"test3")
      amazonS3.putObject(BUCKET_NAME, s"$KEY/test4.txt", s"test4")

      val objectKeys = s3Client.getKeys(BUCKET_NAME, List(s"$KEY/test1.txt", s"$KEY/key2", s"$KEY/test3")).seq
      objectKeys :+ "test5.txt"
      val noOfDeletedObjects = s3Client.deleteObjects(BUCKET_NAME, objectKeys)

      noOfDeletedObjects shouldBe 3
    }

  }

  private def convertStreamToString(inputStream: InputStream): String = {
    val scanner = new Scanner(inputStream).useDelimiter("\\A")
    if (scanner.hasNext) scanner.next else ""
  }
}
