/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.filesystem;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.configuration.Configuration;


/**
 * The PinotFS is intended to be a thin wrapper on top of different filesystems. This interface is intended for internal
 * Pinot use only. This class will be implemented for each pluggable storage type.
 */
public abstract class PinotFS implements Closeable {
  /**
   * Initializes the configurations specific to that filesystem. For instance, any security related parameters can be
   * initialized here and will not be logged.
   */
  public abstract void init(Configuration config);

  /**
   * Creates a new directory. If parent directories are not created, it will create them.
   * If the directory exists, it will return true without doing anything.
   * @throws IOException on IO failure
   */
  public abstract boolean mkdir(URI uri)
      throws IOException;

  /**
   * Deletes the file at the location provided. If the segmentUri is a directory, it will delete the entire directory.
   * @param segmentUri URI of the segment
   * @param forceDelete true if we want the uri and any sub-uris to always be deleted, false if we want delete to fail
   *                    when we want to delete a directory and that directory is not empty
   * @return true if delete is successful else false
   * @throws IOException on IO failure, e.g Uri is not present or not valid
   */
  public abstract boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException;

  /**
   * Moves the file or directory from the src to dst. Does not keep the original file. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories.
   * If both src and dst are files, dst will be overwritten.
   * If src is a file and dst is a directory, src file will get moved under dst directory.
   * If both src and dst are directories, src directory will get moved under dst directory.
   * If src is a directory and dst is a file, operation will fail.
   * For example, if a file /a/b/c is moved to a file /x/y/z, in the case of overwrite, the directory /a/b still exists,
   * but will not contain the file 'c'. Instead, /x/y/z will contain the contents of 'c'.
   * If a file /a is moved to a directory /x/y, all the original files under /x/y will be kept.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @param overwrite true if we want to overwrite the dstURI, false otherwise
   * @return true if move is successful
   * @throws IOException on IO failure
   */
  public abstract boolean move(URI srcUri, URI dstUri, boolean overwrite)
      throws IOException;

  /**
   * Copies the file or directory from the src to dst. The original file is retained. If the dst has parent directories
   * that haven't been created, this method will create all the necessary parent directories.
   * If both src and dst are files, dst will be overwritten.
   * If src is a file and dst is a directory, src file will get copied under dst directory.
   * If both src and dst are directories, src directory will get copied under dst directory.
   * If src is a directory and dst is a file, operation will fail.
   * For example, if a file /x/y/z is copied to /a/b/c, /x/y/z will be retained and /x/y/z will also be present as /a/b/c;
   * if a file /a is copied to a directory /x/y, all the original files under /x/y will be kept.
   * @param srcUri URI of the original file
   * @param dstUri URI of the final file location
   * @return true if copy is successful
   * @throws IOException on IO failure
   */
  public abstract boolean copy(URI srcUri, URI dstUri)
      throws IOException;

  /**
   * Checks whether the file or directory at the provided location exists.
   * @param fileUri URI of file
   * @return true if path exists
   * @throws IOException on IO failure
   */
  public abstract boolean exists(URI fileUri)
      throws IOException;

  /**
   * Returns the length of the file at the provided location.
   * @param fileUri location of file
   * @return the number of bytes
   * @throws IOException on IO failure, e.g if it's a directory.
   */
  public abstract long length(URI fileUri)
      throws IOException;

  /**
   * Lists all the files and directories at the location provided.
   * Lists recursively if {@code recursive} is set to true.
   * Throws IOException if this abstract pathname is not valid, or if an I/O error occurs.
   * @param fileUri location of file
   * @param recursive if we want to list files recursively
   * @return an array of strings that contains file paths
   * @throws IOException on IO failure. See specific implementation
   */
  public abstract String[] listFiles(URI fileUri, boolean recursive)
      throws IOException;

  /**
   * Copies a file from a remote filesystem to the local one. Keeps the original file.
   * @param srcUri location of current file on remote filesystem
   * @param dstFile location of destination on local filesystem
   * @throws Exception if srcUri is not valid or not present, or timeout when downloading file to local
   */
  public abstract void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception;

  /**
   * The src file is on the local disk. Add it to filesystem at the given dst name and the source is kept intact
   * afterwards.
   * @param srcFile location of src file on local disk
   * @param dstUri location of dst on remote filesystem
   * @throws Exception if fileUri is not valid or not present, or timeout when uploading file from local
   */
  public abstract void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception;

  /**
   * Allows us the ability to determine whether the uri is a directory.
   * @param uri location of file or directory
   * @return true if uri is a directory, false otherwise.
   * @throws IOException on IO failure, e.g uri is not valid or not present
   */
  public abstract boolean isDirectory(URI uri)
      throws IOException;

  /**
   * Returns the age of the file
   * @param uri location of file or directory
   * @return A long value representing the time the file was last modified, measured in milliseconds since epoch
   * (00:00:00 GMT, January 1, 1970) or 0L if the file does not exist or if an I/O error occurs
   * @throws IOException if uri is not valid or not present
   */
  public abstract long lastModified(URI uri)
      throws IOException;

  /**
   * Updates the last modified time of an existing file or directory to be current time. If the file system object
   * does not exist, creates an empty file.
   * @param uri location of file or directory
   * @throws IOException if the parent directory doesn't exist
   */
  public abstract boolean touch(URI uri)
      throws IOException;

  /**
   * For certain filesystems, we may need to close the filesystem and do relevant operations to prevent leaks.
   * By default, this method does nothing.
   * @throws IOException on IO failure
   */
  public void close()
      throws IOException {

  }
}
