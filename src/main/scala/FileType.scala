package org.tpch.filetype
sealed trait FileType
case object CSVS3 extends FileType
case object CSVFile extends FileType
case object TBLFile extends FileType
case object TBLHdfs extends FileType
case object TBLHdfsDs extends FileType
case object CSVHdfsDs extends FileType
case object TBLWebHdfs extends FileType
case object CSVWebHdfs extends FileType
case object V1CsvHdfs extends FileType
case object V2CsvHdfs extends FileType
case object TBLS3 extends FileType
case object JDBC extends FileType