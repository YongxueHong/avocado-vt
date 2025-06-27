import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from unittest import mock

from avocado.utils import process
from virttest import utils_disk
from virttest.utils_misc import RandName


class TestIsMount(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="test_is_mount_")
        self.source_file = os.path.join(self.tmpdir, "source_file")
        self.mount_point = os.path.join(self.tmpdir, "mount_point")
        self.linked_mount_point = os.path.join(self.tmpdir, "linked_mount_point")

        # Create a dummy source file and mount point directory
        with open(self.source_file, "w") as f:
            f.write("dummy source")
        os.makedirs(self.mount_point, exist_ok=True)
        os.symlink(self.mount_point, self.linked_mount_point)

        # Use loop device for a mountable source
        self.loop_device = None
        try:
            process.run(f"dd if=/dev/zero of={self.source_file} bs=1M count=10", shell=True)
            process.run(f"mkfs.ext4 {self.source_file}", shell=True)
        except process.CmdError as e:
            self.skipTest(f"Failed to create loop device setup: {e}")


    def tearDown(self):
        # Clean up: unmount if mounted, remove loop device, and remove temp directory
        if self.loop_device:
            try:
                process.run(f"sudo umount {self.mount_point}", shell=True, ignore_status=True)
                process.run(f"sudo umount {self.linked_mount_point}", shell=True, ignore_status=True)
            except process.CmdError:
                pass # Ignore errors during unmount for cleanup
        shutil.rmtree(self.tmpdir)

    def _mount_source(self, mount_target, options=None):
        cmd = f"sudo mount"
        if options:
            cmd += f" -o {options}"
        cmd += f" {self.source_file} {mount_target}"
        try:
            process.run(cmd, shell=True)
        except process.CmdError as e:
            self.fail(f"Failed to mount {self.source_file} at {mount_target}: {e}")

    def _unmount_source(self, mount_target):
        try:
            process.run(f"sudo umount {mount_target}", shell=True, ignore_status=True)
        except process.CmdError:
            pass # Ignore errors during unmount for cleanup

    def test_is_mount_basic(self):
        self._mount_source(self.mount_point)
        self.assertTrue(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, fstype="ext4", verbose=True))
        self._unmount_source(self.mount_point)
        self.assertFalse(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, fstype="ext4", verbose=True))

    def test_is_mount_soft_link_target(self):
        self._mount_source(self.mount_point)
        # Check using the symlink as the destination
        self.assertTrue(utils_disk.is_mount(src=self.source_file, dst=self.linked_mount_point, fstype="ext4", verbose=True))
        self._unmount_source(self.mount_point) # Unmounts the actual mount point
        self.assertFalse(utils_disk.is_mount(src=self.source_file, dst=self.linked_mount_point, fstype="ext4", verbose=True))

    def test_is_mount_with_options(self):
        self._mount_source(self.mount_point, options="ro")
        self.assertTrue(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, options="ro", verbose=True))
        self.assertFalse(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, options="rw", verbose=True)) # Should fail, mounted ro
        self._unmount_source(self.mount_point)

    def test_is_mount_src_only(self):
        self._mount_source(self.mount_point)
        self.assertTrue(utils_disk.is_mount(src=self.source_file, verbose=True))
        self._unmount_source(self.mount_point)
        self.assertFalse(utils_disk.is_mount(src=self.source_file, verbose=True))

    def test_is_mount_dst_only(self):
        self._mount_source(self.mount_point)
        self.assertTrue(utils_disk.is_mount(dst=self.mount_point, verbose=True))
        self._unmount_source(self.mount_point)
        self.assertFalse(utils_disk.is_mount(dst=self.mount_point, verbose=True))

    def test_is_mount_not_mounted(self):
        self.assertFalse(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, verbose=True))

    def test_is_mount_findmnt_fails(self):
        # Simulate findmnt command failure
        with mock.patch('avocado.utils.process.run') as mock_run:
            mock_run.return_value = mock.Mock(exit_status=1, stdout_text="", stderr_text="findmnt error")
            self.assertFalse(utils_disk.is_mount(src=self.source_file, dst=self.mount_point, verbose=True))

    @mock.patch("os.path.realpath")
    def test_is_mount_realpath_handling(self, mock_realpath):
        # Ensure realpath is called for comparisons
        mock_realpath.side_effect = lambda x: x  # simple passthrough
        self._mount_source(self.mount_point)
        utils_disk.is_mount(src=self.source_file, dst=self.mount_point, verbose=True)
        expected_calls = [mock.call(self.source_file), mock.call(self.mount_point)]
        # The number of calls to realpath can vary based on the findmnt output lines,
        # so we check that it's called for the initial src and dst.
        # Each line from findmnt output will also call realpath for mounted_src and mounted_dst
        mock_realpath.assert_any_call(self.source_file)
        mock_realpath.assert_any_call(self.mount_point)
        self._unmount_source(self.mount_point)

if __name__ == "__main__":
    unittest.main()
