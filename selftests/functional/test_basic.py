import json
import os
import shutil
import tempfile
import unittest

from avocado.utils import process, script

from virttest import data_dir

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
BASE_DIR = os.path.abspath(BASE_DIR)

TEST_STATUSES_PY = """from avocado.core import exceptions
from autotest.client.shared import error
import logging

def run(test, params, env):
    result_param = params.get("result_param")

    if result_param == "autotest_skip":
        raise error.TestNAError("my skip")
    elif result_param == "autotest_fail":
        raise error.TestFail("my fail")
    elif result_param == "autotest_error":
        raise error.TestError("my error")
    elif result_param == "other exception":
        raise Exception("asefsadf")
    elif 'skip' in result_param:
        raise exceptions.TestSkipError("Test Skip")
    elif 'pass' in result_param:
        logging.info("Test Pass")
        pass
    elif 'fail' in result_param:
        raise exceptions.TestFail("Test Fail")
    elif 'error' in result_param:
        raise exceptions.TestError("Test Error")
    else:
        pass
"""

TEST_STATUSES_CFG = """variants:
    - test_statuses:
        type = test_statuses
        start_vm = no
        vms = ''
        main_vm = ''
        variants:
            - skip:
                result_param = 'skip'
            - pass:
                result_param = 'pass'
            - fail:
                result_param = 'fail'
            - error:
                result_param = 'error'
            - autotest_skip:
                result_param = 'autotest_skip'
            - autotest_pass:
                result_param = 'autotest_pass'
            - autotest_fail:
                result_param = 'autotest_fail'
            - autotest_error:
                result_param = 'autotest_error'
            - other_exception:
                result_param = "other exception"

only test_statuses
kvm_ver_cmd = /bin/true
kvm_userspace_ver_cmd = /bin/true
verify_host_dmesg = no
"""

TEST_VIRTIO_BLK_VHOST_USER_PY = """
from avocado_vt.test import Test
from avocado_vt.utils import utils_misc

class VirtioBlkVhostUser(Test):
    def test(self):
        # Create a socket for vhost-user
        sock_path = utils_misc.get_tmp_path("vhost-user.sock")
        self.vm.params["chardev_vhost_user_sock_path"] = sock_path

        # Check if VM boots successfully
        self.vm.verify_alive()
"""

TEST_VIRTIO_BLK_VHOST_USER_CFG = """
common:
    # Basic VM configuration
    vm_type = qemu
    main_vm = avocado-vt-vm1
    vms = avocado-vt-vm1
    mem = 1024
    smp = 1
    images = image1
    image_name = Fedora-Cloud-Base-30-1.2.x86_64.qcow2
    image_format = qcow2
    image_size = 10G
    display = none
    nics = nic1
    nic_model = virtio
    nettype = user

    # virtio-blk-vhost-user configuration
    drive_format_image1 = virtio-blk-vhost-user
    chardevs = vhost_user_char
    chardev_backend_vhost_user_char = socket
    chardev_path_vhost_user_char = %(chardev_vhost_user_sock_path)s

variants:
    - virtio_blk_vhost_user:
        type = virtio_blk_vhost_user

only virtio_blk_vhost_user
kvm_ver_cmd = /bin/true
kvm_userspace_ver_cmd = /bin/true
verify_host_dmesg = no
"""


class BasicTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="avocado_" + __name__)
        self.rm_files = []

    def test_statuses(self):
        os.chdir(BASE_DIR)
        test_path = os.path.join(
            data_dir.get_test_providers_dir(),
            "downloads",
            "io-github-autotest-qemu",
            "generic",
            "tests",
            "test_statuses.py",
        )
        self.assertTrue(
            os.path.exists(os.path.dirname(test_path)),
            "The qemu providers dir does not exists, Avocado-vt "
            "is probably not configured properly.",
        )
        self.rm_files.append(test_path)
        script.make_script(test_path, TEST_STATUSES_PY)
        cfg = script.make_script(
            os.path.join(self.tmpdir, "test_statuses.cfg"), TEST_STATUSES_CFG
        )
        result = process.run(
            "avocado --show all run --vt-config %s "
            "--job-results-dir %s" % (cfg, self.tmpdir),
            ignore_status=True,
        )
        self.assertEqual(result.exit_status, 1, "Exit status is not 1:\n%s" % result)
        status = json.load(open(os.path.join(self.tmpdir, "latest", "results.json")))
        act_statuses = [_["status"] for _ in status["tests"]]
        statuses_master = [
            "SKIP",
            "PASS",
            "FAIL",
            "ERROR",
            "CANCEL",
            "PASS",
            "FAIL",
            "ERROR",
            "ERROR",
        ]
        statuses_36lts = [
            "SKIP",
            "PASS",
            "FAIL",
            "ERROR",
            "SKIP",
            "PASS",
            "FAIL",
            "ERROR",
            "ERROR",
        ]
        if not (act_statuses == statuses_master or act_statuses == statuses_36lts):
            self.fail(
                "Test statuses does not match any of expected results:"
                "\nmaster: %s\n36lts: %s\nactual: %s\n\noutput:\n%s"
                % (statuses_master, statuses_36lts, act_statuses, result)
            )

    def tearDown(self):
        for path in self.rm_files:
            try:
                os.unlink(path)
            except IOError:
                pass
        shutil.rmtree(self.tmpdir)

    def test_virtio_blk_vhost_user(self):
        os.chdir(BASE_DIR)
        test_py_path = os.path.join(
            data_dir.get_test_providers_dir(),
            "downloads",
            "io-github-autotest-qemu",
            "generic",
            "tests",
            "virtio_blk_vhost_user.py",
        )
        self.assertTrue(
            os.path.exists(os.path.dirname(test_py_path)),
            "The qemu providers dir does not exist, Avocado-vt "
            "is probably not configured properly.",
        )
        self.rm_files.append(test_py_path)
        script.make_script(test_py_path, TEST_VIRTIO_BLK_VHOST_USER_PY)
        cfg_path = script.make_script(
            os.path.join(self.tmpdir, "virtio_blk_vhost_user.cfg"),
            TEST_VIRTIO_BLK_VHOST_USER_CFG,
        )
        result = process.run(
            "avocado --show all run --vt-config %s "
            "--job-results-dir %s" % (cfg_path, self.tmpdir),
            ignore_status=True,
        )
        self.assertEqual(result.exit_status, 0, "Exit status is not 0:\n%s" % result)
        status = json.load(open(os.path.join(self.tmpdir, "latest", "results.json")))
        self.assertEqual(status["tests"][0]["status"], "PASS",
                         "Test did not pass:\n%s" % result)


if __name__ == "__main__":
    unittest.main()
