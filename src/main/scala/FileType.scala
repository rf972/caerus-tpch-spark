package org.tpch.filetype
sealed trait FileType
case object JDBC extends FileType
case object CSVS3 extends FileType
case object TBLS3 extends FileType
case object CSVFile extends FileType
case object TBLFile extends FileType
case object CSVHdfs extends FileType
case object TBLHdfs extends FileType
case object CSVWebHdfs extends FileType
case object TBLWebHdfs extends FileType
case object CSVHdfsDs extends FileType
case object TBLHdfsDs extends FileType
case object CSVDikeHdfs extends FileType
case object TBLDikeHdfs extends FileType
case object CSVDikeHdfsNoProc extends FileType
case object TBLDikeHdfsNoProc extends FileType
case object CSVWebHdfsDs extends FileType
case object TBLWebHdfsDs extends FileType

/** Object encapsulates methods related to
 *  catigorization of the FileType.
 */
object FileType {
  /** Returns true if this FileType uses
   *  a .tbl format.
   * @param fileType the type to make a determination for.
   * @return true if it is a .tbl format, false otherwise
   */
  def isTbl(fileType: FileType): Boolean = {
    if (fileType == TBLS3 || fileType == TBLFile
      || fileType == TBLHdfs || fileType == TBLWebHdfs
      || fileType == TBLHdfsDs || fileType == TBLDikeHdfs
      || fileType == TBLDikeHdfsNoProc
      || fileType == TBLWebHdfsDs) {
      true
    } else {
      false
    }
  }

  /** returns true if the FileType is one where the
   * data source needs to parse tbl format.
   */
  def isTblToDs(fileType: FileType): Boolean = {
    if (fileType == TBLHdfsDs || fileType == TBLDikeHdfsNoProc
      || fileType == TBLWebHdfsDs || fileType == TBLDikeHdfs) {
      true
    } else {
      false
    }
  }

  /** returns if the FileType is going through the
   * data source (true) or not (false)
   *
   * @param fileType the FileType to check
   * @return true if is data source, false otherwise
   */
  def isTblDatasource(fileType: FileType): Boolean = {
    if (fileType == TBLHdfsDs || fileType == TBLWebHdfsDs
      || fileType == TBLDikeHdfs || fileType == TBLDikeHdfsNoProc) {
      true
    } else {
      false
    }
  }

  /** returns if the FileType is going to use the
   * dike processor (true) or not (false)
   *
   * @param fileType the FileType to check
   * @return true if is using dike processor, false otherwise
   */
  def isDisableProcessor(fileType: FileType): Boolean = {
    if (fileType == CSVDikeHdfsNoProc || fileType == TBLDikeHdfsNoProc) {
      true
    } else {
      false
    }
  }

  /** returns a string related to the type of filesystem for
   *  the purposes of gathering the official hdfs statistics.
   * @param fileType the FileType to check
   * @return String with the statistics type .
   */
  def getStatsType(fileType: FileType): String = {
    if (fileType == CSVHdfs
      || fileType == TBLHdfs
      || fileType == CSVHdfsDs
      || fileType == TBLHdfsDs) {
      "hdfs"
    } else if (fileType == CSVWebHdfs
      || fileType == TBLWebHdfs
      || fileType == CSVWebHdfsDs
      || fileType == TBLWebHdfsDs) {
      "webhdfs"
    } else if (fileType == CSVDikeHdfs
      || fileType == TBLDikeHdfs
      || fileType == CSVDikeHdfsNoProc
      || fileType == TBLDikeHdfsNoProc) {
      "ndphdfs"
    } else if (fileType == CSVFile
      || fileType == TBLFile) {
      "file"
    } else if (fileType == CSVS3
      || fileType == TBLS3) {
      "s3"
    } else if (fileType == JDBC) {
      "jdbc"
    } else {
      "unknown"
    }
  }

  /** returns if the FileType is using hdfs (true) or not (false)
   * @param fileType the FileType to check
   * @return true if is hdfs, false otherwise
   */
  def isHdfs(fileType: FileType): Boolean = {
    if (fileType == TBLHdfs
      || fileType == TBLWebHdfs
      || fileType == TBLHdfsDs
      || fileType == TBLDikeHdfs
      || fileType == TBLDikeHdfsNoProc
      || fileType == CSVHdfs
      || fileType == CSVWebHdfs
      || fileType == CSVHdfsDs
      || fileType == CSVDikeHdfs
      || fileType == CSVDikeHdfsNoProc) {
      true
    } else {
      false
    }
  }

  /** returns if the FileType is going through the
   *  data source (true) or not (false)
   * @param fileType the FileType to check
   * @return true if is data source, false otherwise
   */
  def isDataSource(fileType: FileType): Boolean = {
    if (fileType == JDBC
      || fileType == CSVS3 || fileType == TBLS3
      || fileType == CSVFile || fileType == TBLFile
      || fileType == CSVHdfs || fileType == TBLHdfs
      || fileType == CSVWebHdfs || fileType == TBLWebHdfs) {
      false
    } else {
      true
    }
  }
}
