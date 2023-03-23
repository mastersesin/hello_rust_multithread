// PassthroughFS :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using fuse_mt::FilesystemMT.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::mem;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use fernet;

use crate::libc_extras::libc;
use crate::libc_wrappers;

use reqwest::header::{
    AUTHORIZATION,
    RANGE,
};
use fuse_mt::*;

pub struct PassthroughFS {
    pub target: OsString,
}

const ACCESS_TOKEN: &str = "ya29.a0Ael9sCOhuSL0xHIir8OpMCopbW9piV4FI_WA5YOvFZoIIwLRjL3ClIz4XIYW1suKwofI-lawoFOTRpFz2AoQiCmc6bbzRI88562hZ9vYUVkejj3Lag3uRO0KL7zzYMscwEFltstSkM3c5sGSHMfsnJUcd6-bzl0Y5AaCgYKAS4SARESFQF4udJh17y27BO6-c0mou_c_6nTeQ0169";

fn mode_to_filetype(mode: libc::mode_t) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => { panic!("unknown file type"); }
    }
}

fn stat_to_fuse(stat: libc::stat64) -> FileAttr {
    // st_mode encodes both the kind and the permissions
    let kind = mode_to_filetype(stat.st_mode);
    let perm = (stat.st_mode & 0o7777) as u16;

    let time = |secs: i64, nanos: i64|
        SystemTime::UNIX_EPOCH + Duration::new(secs as u64, nanos as u32);

    // libc::nlink_t is wildly different sizes on different platforms:
    // linux amd64: u64
    // linux x86:   u32
    // macOS amd64: u16
    #[allow(clippy::cast_lossless)]
        let nlink = stat.st_nlink as u32;

    FileAttr {
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: time(stat.st_atime, stat.st_atime_nsec),
        mtime: time(stat.st_mtime, stat.st_mtime_nsec),
        ctime: time(stat.st_ctime, stat.st_ctime_nsec),
        crtime: SystemTime::UNIX_EPOCH,
        kind,
        perm,
        nlink,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        flags: 0,
    }
}

#[cfg(target_os = "macos")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: 0, // TODO
        frsize: 0, // TODO
    }
}

#[cfg(target_os = "linux")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks as u64,
        bfree: statfs.f_bfree as u64,
        bavail: statfs.f_bavail as u64,
        files: statfs.f_files as u64,
        ffree: statfs.f_ffree as u64,
        bsize: statfs.f_bsize as u32,
        namelen: statfs.f_namelen as u32,
        frsize: statfs.f_frsize as u32,
    }
}

impl PassthroughFS {
    fn real_path(&self, partial: &Path) -> OsString {
        PathBuf::from(&self.target)
            .join(partial.strip_prefix("/").unwrap())
            .into_os_string()
    }

    fn stat_real(&self, path: &Path) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(path);
        debug!("stat_real: {:?}", real);

        match libc_wrappers::lstat(real) {
            Ok(stat) => {
                Ok(stat_to_fuse(stat))
            }
            Err(e) => {
                let err = io::Error::from_raw_os_error(e);
                error!("lstat({:?}): {}", path, err);
                Err(err)
            }
        }
    }
}

const TTL: Duration = Duration::from_secs(1);

fn read_data_from_file(file_id: &str, start_byte: i64, end_byte: i64) -> Result<reqwest::blocking::Response, Box<dyn Error>> {
    println!("{} start {} end", start_byte, end_byte);
    let endpoint_url = format!("https://www.googleapis.com/drive/v3/files/{file_id}?supportsAllDrives=true&supportsTeamDrives=true&alt=media");
    let client = reqwest::blocking::Client::new();
    if start_byte >= 0 && end_byte >= 0 {
        let resp = client.get(endpoint_url)
            .header(AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
            .header(RANGE, format!("bytes={start_byte}-{end_byte}"))
            .send()?;
        Ok(resp)
    } else {
        let resp = client.get(endpoint_url)
            .header(AUTHORIZATION, format!("Bearer {ACCESS_TOKEN}"))
            .send()?;
        Ok(resp)
    }
}

fn calc(offset: i64, length: u32) -> (&'static str, i64, i64) {
    let our_map: BTreeMap<i64, &str> =
        BTreeMap::from([(65535, "1xV10xI0QJciPZ0w06S2QoYUYDbom-m6N"),
            (3136277525, "1Ywt29KwAoTw6XKf5edN_rr6Sc97OVakc"),
            (4264895608, "1IxOK2bOqGRz4GwVGx7yPjnAwXq91VGiF"),
            (5683118908, "1vJ60iOmmGPN0Nyc4IUr6YJAy2w-frWrw"),
            (7524698365, "1RXC1L68C1z1IvD3RBp03nlqN9KCZnPM8"),
            (9179191938, "1Cg1YS9GXBW-vAKw17Gky0Q7ak5ccAqZE"),
            (11568039537, "1CZ6y6tU4iF9fChmjcGr3grTnTWcYm25N"),
            (14357437778, "1aI5yJPAFvLjf2TueD0EsYGRkniICj8Ny"),
            (17443531498, "1mu8ukspOhHBeoNV0_Ye2PAmaFGEElsr3"),
            (18707203932, "1SXRmEzwyrRqGkyv2vNtd45AiIj2n7Xmb"),
            (21740857834, "1i_kAThydwWQaR77yQ0jWVQgg9ab7GEs5"),
            (24084580085, "17Chdoaxtg5-cr1fqKCocjP5EcUgyHj90"),
            (25709868851, "1I9RnNzCTd1Yww3PW5lSADO9OkKOAJB5t"),
            (28411019782, "1VjtTOVUmTgCMLV1qdX9l6KaslwOU1vBK"),
            (31146299980, "1Eas0UorI384jTPO9WId_hRYI_WTfOxi0"),
            (32616708307, "1usjXrYrP0Z_fNAn94RLcOIgVqcObH7Xj"),
            (34795775510, "1-zPaTfMFMq5voaFHfF7ltm4te6qzzeFi"),
            (36373250750, "1MOkkdmhcbbU2ZA7Eld2xpf-QFgcvJwBW"),
            (38307686165, "1k0QVfXJuB3miX1OHXlfNI9FcqxAGWUup"),
            (40566606366, "1OeUBxegGYhIJqduxCmqzOmfTGfWXi3Pz"),
            (43109648516, "1nMwc3zinrjZZPfDN5X15f3oeC9BMfvZS"),
            (46303643299, "1I9lBTA1HTaqRlxF9-tY2REGEIagDbpJD"),
            (49324970002, "1r376mJJ1FCEINS78DC_wj5hDwdcilwNY"),
            (51146338717, "1SbeMFjhdba5kQJ31yASpxic6LEmWO_Dj"),
            (53734141657, "1iYR8jF4qmTw2DvA7tnQMUwjFM7Gls4Iw"),
            (56840465632, "1BnMfN-ASfROnXiVxJJvFTDUMgOH7yMo_"),
            (57916633250, "1o36WXUYsOYE2rvG6Oiv95DlPpYj1sK8m"),
            (60850437032, "1izQXyjH06P_gY9QQhe2sysvRErp_E3YP"),
            (63182086659, "1PzvsvvcEm1FGcksPA73dacOF0d6ZpIBu"),
            (64894284328, "1wdcAtZIuiHavkqWgVhm3RcqaPqDFllVA"),
            (67145810297, "1kz1JJBLwwSBb5cTI6hzyF2FdbM41Hrxc"),
            (69655104876, "1ZhybvmT1CuH2lmh9-ELyKxt9sHIwSXzr"),
            (72687779417, "1_pMK7M_kSFeSYEB0SpZ78kBoIPndj7oX"),
            (75313290690, "1QVY_dIPC8pXp3dv4dyIUP_-y92S6lj7H"),
            (78419280338, "10JqukZYupv88e6oQFaG59jKB6URyP-M5"),
            (80480908546, "16DbRqXvSvCIaFQ8ogmmfGxEFLthIU31x"),
            (83388003875, "177JNnu7vaCXD-lxWQ11rsxexyLllh_ZX"),
            (85214586791, "1HbPsOwbUBEeWNUUJkbFQ2_xJ5ChXwPFw"),
            (86895533949, "1tAKEtny2zB7oBQrCTReQ3CfZ2Y0UksU_"),
            (88467589447, "1tfsrg1c0EVcLt70GSppDQxolWVVgz-Go"),
            (90524897624, "1Ref5HYXdeplmYlyMTAw1dfe9hVqcVQ9T"),
            (92701391675, "1LjDwNHxzWMUWoKNxpT0kXZpHbnRWp7mN"),
            (95705418734, "1JU601nRmgfqOkkuunZD0c6W4XvBsAGLx"),
            (98283004808, "18qEFYin7Ul8VHgDqf-UpZk4XZBBymAkF"),
            (100655737957, "1ycPtJi1WI8etHx_stsgl-sDTSZDjZHrb"),
            (102157030373, "1_zs0WIKPar1gAoAQ0mitdiTZWrczcSyi"),
            (105252025064, "1goafZpkSCIZf-gfoA9JIaHmhze7Ip4MQ"),
            (108035212644, "1H2CRA3pCJ7VbK9741meArYzEQ3q2fnXF"),
            (108797856195, "1dhFt7Lxxs0Y8nTQeSmlMs-J7CGG6nbGZ"), ]
        );
    let mut current_start_byte = 0;
    for (end_byte, file_id) in &our_map {
        // println!("{} end_byte", end_byte);
        let x = offset;
        let y = offset + length as i64 - 1;
        if current_start_byte <= x && y <= *end_byte {
            return (file_id, x - current_start_byte, y - current_start_byte);
        }
        current_start_byte = *end_byte + 1
    }
    return ("", 0, 0);
}

impl FilesystemMT for PassthroughFS {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        debug!("init");
        Ok(())
    }

    fn destroy(&self) {
        debug!("destroy");
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
        debug!("getattr: {:?}", path);

        if let Some(fh) = fh {
            match libc_wrappers::fstat(fh) {
                Ok(stat) => Ok((TTL, stat_to_fuse(stat))),
                Err(e) => Err(e)
            }
        } else {
            match self.stat_real(path) {
                Ok(attr) => Ok((TTL, attr)),
                Err(e) => Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let real = self.real_path(path);
        debug!("opendir: {:?} (flags = {:#o})", real, _flags);
        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh, 0)),
            Err(e) => {
                let ioerr = io::Error::from_raw_os_error(e);
                error!("opendir({:?}): {}", path, ioerr);
                Err(e)
            }
        }
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        libc_wrappers::closedir(fh)
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
        debug!("readdir: {:?}", path);
        let mut entries: Vec<DirectoryEntry> = vec![];

        if fh == 0 {
            error!("readdir: missing fh");
            return Err(libc::EINVAL);
        }

        loop {
            match libc_wrappers::readdir(fh) {
                Ok(Some(entry)) => {
                    let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                    let name = OsStr::from_bytes(name_c.to_bytes()).to_owned();

                    let filetype = match entry.d_type {
                        libc::DT_DIR => FileType::Directory,
                        libc::DT_REG => FileType::RegularFile,
                        libc::DT_LNK => FileType::Symlink,
                        libc::DT_BLK => FileType::BlockDevice,
                        libc::DT_CHR => FileType::CharDevice,
                        libc::DT_FIFO => FileType::NamedPipe,
                        libc::DT_SOCK => {
                            warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
                            FileType::NamedPipe
                        }
                        _ => {
                            let entry_path = PathBuf::from(path).join(&name);
                            let real_path = self.real_path(&entry_path);
                            match libc_wrappers::lstat(real_path) {
                                Ok(stat64) => mode_to_filetype(stat64.st_mode),
                                Err(errno) => {
                                    let ioerr = io::Error::from_raw_os_error(errno);
                                    panic!("lstat failed after readdir_r gave no file type for {:?}: {}",
                                           entry_path, ioerr);
                                }
                            }
                        }
                    };

                    entries.push(DirectoryEntry {
                        name,
                        kind: filetype,
                    })
                }
                Ok(None) => { break; }
                Err(e) => {
                    error!("readdir: {:?}: {}", path, e);
                    return Err(e);
                }
            }
        }

        Ok(entries)
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        debug!("open: {:?} flags={:#x}", path, flags);

        let real = self.real_path(path);
        match libc_wrappers::open(real, flags as libc::c_int) {
            Ok(fh) => Ok((fh, flags)),
            Err(e) => {
                error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn release(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        debug!("release: {:?}", path);
        libc_wrappers::close(fh)
    }

    fn read(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64, size: u32, callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult) -> CallbackResult {
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let (file_id, start_byte, end_byte) = calc(offset as i64, size);
        if offset < 64 * 1024 {
            match read_data_from_file(file_id, -1, -1) {
                Ok(data) => {
                    let key = "E-bxU5geNyrojsSg2mqn5Yv1_veAczf0xaffrFJBSjk=";
                    let fernet_obj = fernet::Fernet::new(&key).unwrap();
                    let decrypted_data = fernet_obj.decrypt(&data.text().unwrap()).unwrap();
                    callback(Ok(&decrypted_data.as_slice()[offset as usize..(offset as i64 + size as i64 - 1) as usize]))
                }
                Err(e) => { callback(Err(0)) }
            }
        } else {
            match read_data_from_file(file_id, start_byte as i64, end_byte as i64) {
                Ok(data) => {
                    // println!("{}", &data.bytes().unwrap().len());
                    // reply.data(&data.text().unwrap().as_bytes()[..size as usize]);
                    let resp_data = &data.bytes().unwrap();
                    callback(Ok(&resp_data))
                    // println!("{:x?}", resp_data)
                }

                Err(e) => { callback(Err(0)) }
            }
        }
    }

    fn write(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return Err(e.raw_os_error().unwrap());
        }
        let nwritten: u32 = match file.write(&data) {
            Ok(n) => n as u32,
            Err(e) => {
                error!("write {:?}, {:#x} @ {:#x}: {}", path, data.len(), offset, e);
                return Err(e.raw_os_error().unwrap());
            }
        };

        Ok(nwritten)
    }

    fn flush(&self, _req: RequestInfo, path: &Path, fh: u64, _lock_owner: u64) -> ResultEmpty {
        debug!("flush: {:?}", path);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.flush() {
            error!("flush({:?}): {}", path, e);
            return Err(e.raw_os_error().unwrap());
        }

        Ok(())
    }

    fn fsync(&self, _req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
        debug!("fsync: {:?}, data={:?}", path, datasync);
        let file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = if datasync {
            file.sync_data()
        } else {
            file.sync_all()
        } {
            error!("fsync({:?}, {:?}): {}", path, datasync, e);
            return Err(e.raw_os_error().unwrap());
        }

        Ok(())
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, mode: u32) -> ResultEmpty {
        debug!("chmod: {:?} to {:#o}", path, mode);

        let result = if let Some(fh) = fh {
            unsafe { libc::fchmod(fh as libc::c_int, mode as libc::mode_t) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chmod(path_c.as_ptr(), mode as libc::mode_t)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chmod({:?}, {:#o}): {}", path, mode, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn chown(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, uid: Option<u32>, gid: Option<u32>) -> ResultEmpty {
        let uid = uid.unwrap_or(::std::u32::MAX);   // docs say "-1", but uid_t is unsigned
        let gid = gid.unwrap_or(::std::u32::MAX);
        // ditto for gid_t
        debug!("chown: {:?} to {}:{}", path, uid, gid);

        let result = if let Some(fd) = fh {
            unsafe { libc::fchown(fd as libc::c_int, uid, gid) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chown(path_c.as_ptr(), uid, gid)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chown({:?}, {}, {}): {}", path, uid, gid, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate: {:?} to {:#x}", path, size);

        let result = if let Some(fd) = fh {
            unsafe { libc::ftruncate64(fd as libc::c_int, size as i64) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::truncate64(path_c.as_ptr(), size as i64)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("truncate({:?}, {}): {}", path, size, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn utimens(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, atime: Option<SystemTime>, mtime: Option<SystemTime>) -> ResultEmpty {
        debug!("utimens: {:?}: {:?}, {:?}", path, atime, mtime);

        let systemtime_to_libc = |time: Option<SystemTime>| -> libc::timespec {
            if let Some(time) = time {
                let (secs, nanos) = match time.duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos()),
                    Err(in_past) => {
                        let duration = in_past.duration();
                        (-(duration.as_secs() as i64), duration.subsec_nanos())
                    }
                };

                libc::timespec {
                    tv_sec: secs,
                    tv_nsec: i64::from(nanos),
                }
            } else {
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                }
            }
        };

        let times = [systemtime_to_libc(atime), systemtime_to_libc(mtime)];

        let result = if let Some(fd) = fh {
            unsafe { libc::futimens(fd as libc::c_int, &times as *const libc::timespec) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), &times as *const libc::timespec, libc::AT_SYMLINK_NOFOLLOW)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("utimens({:?}, {:?}, {:?}): {}", path, atime, mtime, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        debug!("readlink: {:?}", path);

        let real = self.real_path(path);
        match ::std::fs::read_link(real) {
            Ok(target) => Ok(target.into_os_string().into_vec()),
            Err(e) => Err(e.raw_os_error().unwrap()),
        }
    }

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        debug!("statfs: {:?}", path);

        let real = self.real_path(path);
        let mut buf: libc::statfs = unsafe { ::std::mem::zeroed() };
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.into_vec());
            libc::statfs(path_c.as_ptr(), &mut buf)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("statfs({:?}): {}", path, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(statfs_to_fuse(buf))
        }
    }

    fn fsyncdir(&self, _req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
        debug!("fsyncdir: {:?} (datasync = {:?})", path, datasync);

        // TODO: what does datasync mean with regards to a directory handle?
        let result = unsafe { libc::fsync(fh as libc::c_int) };
        if -1 == result {
            let e = io::Error::last_os_error();
            error!("fsyncdir({:?}): {}", path, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn mknod(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, mode: u32, rdev: u32) -> ResultEntry {
        debug!("mknod: {:?}/{:?} (mode={:#o}, rdev={})", parent_path, name, mode, rdev);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mknod(path_c.as_ptr(), mode as libc::mode_t, rdev as libc::dev_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mknod({:?}, {}, {}): {}", real, mode, rdev, e);
            Err(e.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => Err(e),   // if this happens, yikes
            }
        }
    }

    fn mkdir(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        debug!("mkdir {:?}/{:?} (mode={:#o})", parent_path, name, mode);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mkdir(path_c.as_ptr(), mode as libc::mode_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mkdir({:?}, {:#o}): {}", real, mode, e);
            Err(e.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                Err(e) => {
                    error!("lstat after mkdir({:?}, {:#o}): {}", real, mode, e);
                    Err(e)   // if this happens, yikes
                }
            }
        }
    }

    fn unlink(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink {:?}/{:?}", parent_path, name);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        fs::remove_file(&real)
            .map_err(|ioerr| {
                error!("unlink({:?}): {}", real, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn rmdir(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("rmdir: {:?}/{:?}", parent_path, name);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        fs::remove_dir(&real)
            .map_err(|ioerr| {
                error!("rmdir({:?}): {}", real, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn symlink(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, target: &Path) -> ResultEntry {
        debug!("symlink: {:?}/{:?} -> {:?}", parent_path, name, target);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        match ::std::os::unix::fs::symlink(target, &real) {
            Ok(()) => {
                match libc_wrappers::lstat(real.clone().into_os_string()) {
                    Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                    Err(e) => {
                        error!("lstat after symlink({:?}, {:?}): {}", real, target, e);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                error!("symlink({:?}, {:?}): {}", real, target, e);
                Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn rename(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, newparent_path: &Path, newname: &OsStr) -> ResultEmpty {
        debug!("rename: {:?}/{:?} -> {:?}/{:?}", parent_path, name, newparent_path, newname);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let newreal = PathBuf::from(self.real_path(newparent_path)).join(newname);
        fs::rename(&real, &newreal)
            .map_err(|ioerr| {
                error!("rename({:?}, {:?}): {}", real, newreal, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
        debug!("link: {:?} -> {:?}/{:?}", path, newparent, newname);

        let real = self.real_path(path);
        let newreal = PathBuf::from(self.real_path(newparent)).join(newname);
        match fs::hard_link(&real, &newreal) {
            Ok(()) => {
                match libc_wrappers::lstat(real.clone()) {
                    Ok(attr) => Ok((TTL, stat_to_fuse(attr))),
                    Err(e) => {
                        error!("lstat after link({:?}, {:?}): {}", real, newreal, e);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                error!("link({:?}, {:?}): {}", real, newreal, e);
                Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32, flags: u32) -> ResultCreate {
        debug!("create: {:?}/{:?} (mode={:#o}, flags={:#x})", parent, name, mode, flags);

        let real = PathBuf::from(self.real_path(parent)).join(name);
        let fd = unsafe {
            let real_c = CString::from_vec_unchecked(real.clone().into_os_string().into_vec());
            libc::open(real_c.as_ptr(), flags as i32 | libc::O_CREAT | libc::O_EXCL, mode)
        };

        if -1 == fd {
            let ioerr = io::Error::last_os_error();
            error!("create({:?}): {}", real, ioerr);
            Err(ioerr.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok(CreatedEntry {
                    ttl: TTL,
                    attr: stat_to_fuse(attr),
                    fh: fd as u64,
                    flags,
                }),
                Err(e) => {
                    error!("lstat after create({:?}): {}", real, io::Error::from_raw_os_error(e));
                    Err(e)
                }
            }
        }
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        debug!("listxattr: {:?}", path);

        let real = self.real_path(path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            let nread = libc_wrappers::llistxattr(
                real, unsafe { mem::transmute(data.spare_capacity_mut()) })?;
            unsafe { data.set_len(nread) };
            Ok(Xattr::Data(data))
        } else {
            let nbytes = libc_wrappers::llistxattr(real, &mut [])?;
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        debug!("getxattr: {:?} {:?} {}", path, name, size);

        let real = self.real_path(path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            let nread = libc_wrappers::lgetxattr(
                real, name.to_owned(), unsafe { mem::transmute(data.spare_capacity_mut()) })?;
            unsafe { data.set_len(nread) };
            Ok(Xattr::Data(data))
        } else {
            let nbytes = libc_wrappers::lgetxattr(real, name.to_owned(), &mut [])?;
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn setxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, value: &[u8], flags: u32, position: u32) -> ResultEmpty {
        debug!("setxattr: {:?} {:?} {} bytes, flags = {:#x}, pos = {}", path, name, value.len(), flags, position);
        let real = self.real_path(path);
        libc_wrappers::lsetxattr(real, name.to_owned(), value, flags, position)
    }

    fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("removexattr: {:?} {:?}", path, name);
        let real = self.real_path(path);
        libc_wrappers::lremovexattr(real, name.to_owned())
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: RequestInfo, name: &OsStr) -> ResultEmpty {
        info!("setvolname: {:?}", name);
        Err(libc::ENOTSUP)
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: RequestInfo, path: &Path) -> ResultXTimes {
        debug!("getxtimes: {:?}", path);
        let xtimes = XTimes {
            bkuptime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
        };
        Ok(xtimes)
    }
}

/// A file that is not closed upon leaving scope.
struct UnmanagedFile {
    inner: Option<File>,
}

impl UnmanagedFile {
    unsafe fn new(fd: u64) -> UnmanagedFile {
        UnmanagedFile {
            inner: Some(File::from_raw_fd(fd as i32))
        }
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_all()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_data()
    }
}

impl Drop for UnmanagedFile {
    fn drop(&mut self) {
        // Release control of the file descriptor so it is not closed.
        let file = self.inner.take().unwrap();
        file.into_raw_fd();
    }
}

impl Read for UnmanagedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read(buf)
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read_to_end(buf)
    }
}

impl Write for UnmanagedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.as_ref().unwrap().flush()
    }
}

impl Seek for UnmanagedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.as_ref().unwrap().seek(pos)
    }
}
