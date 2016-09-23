/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.fs.legacy;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.fs.StorageContext;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MetaUtils;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.backup.HFileArchiver;

@InterfaceAudience.Private
public class LegacyMasterFileSystem extends MasterStorage<LegacyPathIdentifier> {
  private static final Log LOG = LogFactory.getLog(LegacyMasterFileSystem.class);

  private final Path sidelineDir;
  private final Path snapshotDir;
  private final Path archiveDataDir;
  private final Path archiveDir;
  private final Path tmpDataDir;
  private final Path dataDir;
  private final Path tmpDir;

  public LegacyMasterFileSystem(Configuration conf, FileSystem fs, LegacyPathIdentifier rootDir) {
    super(conf, fs, rootDir);

    // base directories
    this.sidelineDir = LegacyLayout.getSidelineDir(rootDir.path);
    this.snapshotDir = LegacyLayout.getSnapshotDir(rootDir.path);
    this.archiveDir = LegacyLayout.getArchiveDir(rootDir.path);
    this.archiveDataDir = LegacyLayout.getDataDir(this.archiveDir);
    this.dataDir = LegacyLayout.getDataDir(rootDir.path);
    this.tmpDir = LegacyLayout.getTempDir(rootDir.path);
    this.tmpDataDir = LegacyLayout.getDataDir(this.tmpDir);
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException {
    getFileSystem().mkdirs(getNamespaceDir(StorageContext.DATA, nsDescriptor.getName()));
  }

  public void deleteNamespace(String namespaceName) throws IOException {
    FileSystem fs = getFileSystem();
    Path nsDir = getNamespaceDir(StorageContext.DATA, namespaceName);

    try {
      for (FileStatus status : fs.listStatus(nsDir)) {
        if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
          throw new IOException("Namespace directory contains table dir: " + status.getPath());
        }
      }
      if (!fs.delete(nsDir, true)) {
        throw new IOException("Failed to remove namespace: " + namespaceName);
      }
    } catch (FileNotFoundException e) {
      // File already deleted, continue
      LOG.debug("deleteDirectory throws exception: " + e);
    }
  }

  public Collection<String> getNamespaces(StorageContext ctx) throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(), getNamespaceDir(ctx));
    if (stats == null) return Collections.emptyList();

    ArrayList<String> namespaces = new ArrayList<String>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      namespaces.add(stats[i].getPath().getName());
    }
    return namespaces;
  }

  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================s
  @Override
  public boolean createTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return LegacyTableDescriptor.createTableDescriptor(getFileSystem(),
      getTableDir(ctx, tableDesc.getTableName()), tableDesc, force);
  }

  @Override
  public void updateTableDescriptor(StorageContext ctx, HTableDescriptor tableDesc) throws IOException {
    LegacyTableDescriptor.updateTableDescriptor(getFileSystem(),
        getTableDir(ctx, tableDesc.getTableName()), tableDesc);
  }

  @Override
  public HTableDescriptor getTableDescriptor(StorageContext ctx, TableName tableName)
      throws IOException {
    return LegacyTableDescriptor.getTableDescriptorFromFs(
        getFileSystem(), getTableDir(ctx, tableName));
  }

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================
  @Override
  public void deleteTable(StorageContext ctx, TableName tableName) throws IOException {
    Path tableDir = getTableDir(ctx, tableName);
    if (!FSUtils.deleteDirectory(getFileSystem(), tableDir)) {
      throw new IOException("Failed delete of " + tableName);
    }
  }

  @Override
  public Collection<TableName> getTables(StorageContext ctx, String namespace)
      throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(),
        getNamespaceDir(ctx, namespace), new FSUtils.UserTableDirFilter(getFileSystem()));
    if (stats == null) return Collections.emptyList();

    ArrayList<TableName> tables = new ArrayList<TableName>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      tables.add(TableName.valueOf(namespace, stats[i].getPath().getName()));
    }
    return tables;
  }

  // ==========================================================================
  //  PUBLIC Methods - Table Regions related
  // ==========================================================================
  @Override
  public Collection<HRegionInfo> getRegions(StorageContext ctx, TableName tableName)
      throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(),
        getTableDir(ctx, tableName), new FSUtils.RegionDirFilter(getFileSystem()));
    if (stats == null) return Collections.emptyList();

    ArrayList<HRegionInfo> regions = new ArrayList<HRegionInfo>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      regions.add(loadRegionInfo(stats[i].getPath()));
    }
    return regions;
  }

  protected HRegionInfo loadRegionInfo(Path regionDir) throws IOException {
    FSDataInputStream in = getFileSystem().open(LegacyLayout.getRegionInfoFile(regionDir));
    try {
      return HRegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
  }

  // ==========================================================================
  //  PROTECTED Methods - Bootstrap
  // ==========================================================================

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it.
   * Create hbase.version and the hbase:meta directory if not one.
   * </li>
   * <li>Create a log archive directory for RS to put archived logs</li>
   * </ol>
   * Idempotent.
   * @throws IOException
   */
  @Override
  protected ClusterId startup() throws IOException {
    Configuration c = getConfiguration();
    Path rc = ((LegacyPathIdentifier)getRootContainer()).path;
    FileSystem fs = getFileSystem();

    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));

    boolean isSecurityEnabled = "kerberos".equalsIgnoreCase(c.get("hbase.security.authentication"));
    FsPermission rootDirPerms = new FsPermission(c.get("hbase.rootdir.perms", "700"));

    // Filesystem is good. Go ahead and check for hbase.rootdir.
    try {
      if (!fs.exists(rc)) {
        if (isSecurityEnabled) {
          fs.mkdirs(rc, rootDirPerms);
        } else {
          fs.mkdirs(rc);
        }
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rc, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
            10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      } else {
        if (!fs.isDirectory(rc)) {
          throw new IllegalArgumentException(rc.toString() + " is not a directory");
        }
        if (isSecurityEnabled && !rootDirPerms.equals(fs.getFileStatus(rc).getPermission())) {
          // check whether the permission match
          LOG.warn("Found rootdir permissions NOT matching expected \"hbase.rootdir.perms\" for "
              + "rootdir=" + rc.toString() + " permissions=" + fs.getFileStatus(rc).getPermission()
              + " and  \"hbase.rootdir.perms\" configured as "
              + c.get("hbase.rootdir.perms", "700") + ". Automatically setting the permissions. You"
              + " can change the permissions by setting \"hbase.rootdir.perms\" in hbase-site.xml "
              + "and restarting the master");
          fs.setPermission(rc, rootDirPerms);
        }
        // as above
        FSUtils.checkVersion(fs, rc, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
            10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    } catch (DeserializationException de) {
      LOG.fatal("Please fix invalid configuration for " + HConstants.HBASE_DIR, de);
      IOException ioe = new IOException();
      ioe.initCause(de);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      LOG.fatal("Please fix invalid configuration for "
          + HConstants.HBASE_DIR + " " + rc.toString(), iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rc, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rc, new ClusterId(), c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10
          * 1000));
    }
    return FSUtils.getClusterId(fs, rc);
  }

  @Override
  public void logStorageState(Log log) throws IOException {
    FSUtils.logFileSystemState(getFileSystem(), ((LegacyPathIdentifier)getRootContainer()).path,
        LOG);
  }

  @Override
  protected void bootstrapMeta() throws IOException {
    // TODO ask RegionStorage
    if (!FSUtils.metaRegionExists(getFileSystem(), getRootContainer().path)) {
      bootstrapMeta(getConfiguration());
    }

    // Create tableinfo-s for hbase:meta if not already there.
    // assume, created table descriptor is for enabling table
    // meta table is a system table, so descriptors are predefined,
    // we should get them from registry.
    createTableDescriptor(HTableDescriptor.metaTableDescriptor(getConfiguration()), false);
  }

  private static void bootstrapMeta(final Configuration c) throws IOException {
    LOG.info("BOOTSTRAP: creating hbase:meta region");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstrap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      HTableDescriptor metaDescriptor = HTableDescriptor.metaTableDescriptor(c);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, false);
      HRegion meta = HRegion.createHRegion(c, metaDescriptor, metaHRI, null);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, true);
      meta.close();
    } catch (IOException e) {
        e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  @Override
  protected void startupCleanup() throws IOException {
    checkTempDir(getTempContainer().path, getConfiguration(), getFileSystem());
  }

  /**
   * Make sure the hbase temp directory exists and is empty.
   * NOTE that this method is only executed once just after the master becomes the active one.
   */
  private void checkTempDir(final Path tmpdir, final Configuration c, final FileSystem fs)
      throws IOException {
    // If the temp directory exists, clear the content (left over, from the previous run)
    if (fs.exists(tmpdir)) {
      // Archive table in temp, maybe left over from failed deletion,
      // if not the cleaner will take care of them.
      for (Path tabledir: FSUtils.getTableDirs(fs, tmpdir)) {
        for (Path regiondir: FSUtils.getRegionDirs(fs, tabledir)) {
          HFileArchiver.archiveRegion(fs, getRootContainer().path, tabledir, regiondir);
        }
      }
      if (!fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
    if (!fs.mkdirs(tmpdir)) {
      throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
    }
  }

  // ==========================================================================
  //  PROTECTED Methods - Path
  // ==========================================================================
  protected Path getNamespaceDir(StorageContext ctx) {
    return getBaseDirFromContext(ctx);
  }

  protected Path getNamespaceDir(StorageContext ctx, String namespace) {
    return LegacyLayout.getNamespaceDir(getBaseDirFromContext(ctx), namespace);
  }

  protected Path getTableDir(StorageContext ctx, TableName table) {
    return LegacyLayout.getTableDir(getBaseDirFromContext(ctx), table);
  }

  protected Path getRegionDir(StorageContext ctx, TableName table, HRegionInfo hri) {
    return LegacyLayout.getRegionDir(getTableDir(ctx, table), hri);
  }

  @Override
  public LegacyPathIdentifier getTempContainer() {
    return new LegacyPathIdentifier(tmpDir);
  }

  protected Path getBaseDirFromContext(StorageContext ctx) {
    switch (ctx) {
      case TEMP: return tmpDataDir;
      case DATA: return dataDir;
      case ARCHIVE: return archiveDataDir;
      case SNAPSHOT: return snapshotDir;
      case SIDELINE: return sidelineDir;
      default: throw new RuntimeException("Invalid context: " + ctx);
    }
  }
}
