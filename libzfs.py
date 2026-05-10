# Shim libzfs para zettarepl en Linux
# Implementa la API de FreeBSD usando el CLI de zfs
import enum
import subprocess

class ZFSException(Exception):
    pass

class SendFlag(enum.Enum):
    REPLICATE  = "replicate"
    DOALL      = "doall"
    PROPS      = "props"
    DEDUP      = "dedup"
    LARGEBLOCK = "largeblock"
    EMBED_DATA = "embed"
    COMPRESS   = "compress"
    RAW        = "raw"

class ZFSDataset:
    def __init__(self, name):
        self.name = name

    def send(self, fh, fromname=None, toname=None, flags=None):
        if flags is None:
            flags = set()
        cmd = ["zfs", "send"]
        if SendFlag.REPLICATE  in flags: cmd.append("-R")
        if SendFlag.PROPS      in flags: cmd.append("-p")
        if SendFlag.DEDUP      in flags: cmd.append("-D")
        if SendFlag.LARGEBLOCK in flags: cmd.append("-L")
        if SendFlag.EMBED_DATA in flags: cmd.append("-e")
        if SendFlag.COMPRESS   in flags: cmd.append("-c")
        if SendFlag.RAW        in flags: cmd.append("-w")
        if SendFlag.DOALL      in flags: cmd.append("-I")
        if fromname:
            cmd.extend(["-i", fromname])
        cmd.append(f"{self.name}@{toname}")
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise ZFSException(result.stderr.decode().strip())

class ZFS:
    def get_object(self, name):
        return ZFSDataset(name)

    def receive(self, dataset, fh, force=False, nomount=False, resumable=False, props=None):
        cmd = ["zfs", "receive"]
        if force:     cmd.append("-F")
        if nomount:   cmd.append("-u")
        if resumable: cmd.append("-s")
        if props:
            for k, v in props.items():
                if v is None or str(v) == "None":
                    cmd.extend(["-x", k])
                else:
                    cmd.extend(["-o", f"{k}={v}"])
        cmd.append(dataset)
        result = subprocess.run(cmd, stdin=fh, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise ZFSException(result.stderr.decode().strip())

    def send_resume(self, fh, token, flags=None):
        if flags is None:
            flags = set()
        cmd = ["zfs", "send"]
        if SendFlag.LARGEBLOCK in flags: cmd.append("-L")
        if SendFlag.EMBED_DATA in flags: cmd.append("-e")
        if SendFlag.COMPRESS   in flags: cmd.append("-c")
        if SendFlag.RAW        in flags: cmd.append("-w")
        cmd.extend(["-t", token])
        result = subprocess.run(cmd, stdout=fh, stderr=subprocess.PIPE)
        if result.returncode != 0:
            raise ZFSException(result.stderr.decode().strip())
