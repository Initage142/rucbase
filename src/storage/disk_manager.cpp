/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager.h"

#include <assert.h>    // for assert
#include <string.h>    // for memset
#include <sys/stat.h>  // for stat
#include <unistd.h>    // for lseek

#include "defs.h"

DiskManager::DiskManager() { memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char))); }

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等 时 throw InternalError("DiskManager::write_page Error");
        lseek(fd,page_no*PAGE_SIZE,SEEK_SET) ;

        ssize_t bytes_written = write(fd,offset,num_bytes);
        if(bytes_written != num_bytes){
            throw InternalError("DiskManager::write_page Error");
        }

}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager::read_page(int fd, page_id_t page_no, char *offset, int num_bytes) {
    // Todo:
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager::read_page Error");
        lseek(fd,page_no*PAGE_SIZE,SEEK_SET);
        
        ssize_t bytes_read = read(fd,offset,num_bytes);

        if(bytes_read != num_bytes){
            throw InternalError("DiskManager::write_page Error");
        }


}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager::allocate_page(int fd) {
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager::is_dir(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}
// stat()是通过文件名来获取文件的属性信息，并将这些信息存储在一个struct stat结构体中当调用成功时，stat函数返回0，失败时返回-1，此时错误代码存储在errno中。

void DiskManager::create_dir(const std::string &path) {
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager::destroy_dir(const std::string &path) {
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true 
 * @param {string} &path 指定路径文件
 */
bool DiskManager::is_file(const std::string &path) {
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager::create_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    //因为当同时使用O_CREAT和O_EXCL的时候，如果文件已经存在，open会失败，返回-1，这样可以确保不会覆盖已有文件，同时只在文件不存在时创建新文件。
    if(is_file(path)){
        throw FileExistsError(path);
    }

    int fd = open(path.c_str(),O_CREAT|O_WRONLY|O_EXCL,0644);
    if(fd == -1){
        throw InternalError("DisManager::create_file: Open Error"); //成功创建后关闭文件描述符
    }
    if(close(fd) == -1){
        throw InternalError("DisManager::create_file: Close Error");
    }
      /*(1)资源管理
         open() 返回的文件描述符 (fd) 是系统资源，数量有限（受 ulimit -n 限制）。
        若不关闭，多次调用 create_file() 可能导致文件描述符耗尽，引发程序崩溃或系统错误。
        (2)写入后确保数据持久化
        虽然此函数仅创建空文件，但若未来扩展功能写入数据，close() 会触发内核将缓冲区数据真正写入磁盘当前场景中，空文件无数据，但关闭仍是良好实践。）
        (3)避免跨进程冲突
        未关闭的 fd 可能被其他进程继承（如通过 fork()），导致文件锁定或意外操作。*/

}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager::destroy_file(const std::string &path) {
    // Todo:
    // 调用unlink()函数
    // 注意不能删除未关闭的文件

    //当有其他进程打开了要删除的文件时，unlink函数 不会立即删除该文件。它会等到所有打开该文件的进程都关闭后，文件才会被删除。
    if(!is_file(path)){
        throw FileNotFoundError(path);
    }
    if(path2fd_.count(path)){
        throw  FileNotClosedError(path);
    }

    if(unlink(path.c_str()) == -1)
    {
        throw InternalError("DisManager::destroy_file: Unlink Error");
    }

    
}


/**
 * @description: 打开指定路径文件 
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager::open_file(const std::string &path) {
    // Todo:
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    
    //检查文件是否已打开
    if(!is_file(path)){
        throw FileNotFoundError(path);
    }

    if(path2fd_.find(path) != path2fd_.end())
    {
        throw FileNotClosedError(path);
    }
    int fd = open(path.c_str(),O_RDWR);
    if(fd == -1){
    //  FileNotFoundError(path);
        throw InternalError("DiskManager::open_file: Open Error");
    }
    // if(this->path2fd_.count(path)) {  // 已打开则不重复打开
    //     throw FileNotClosedError(path);
    // }
    // if(!this->is_file(path)) { // path是否正确
    //     throw FileNotFoundError(path);
    // }
    // int fd = open(path.c_str(), O_RDWR);
    // if(fd < 0) {
    //     throw UnixError();
    // }
    
    path2fd_[path] = fd;
    fd2path_[fd] = path;

    return fd;

}

/**
 * @description:用于关闭指定路径文件 
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager::close_file(int fd) {
    // Todo:
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    auto fd_it = fd2path_.find(fd);
    //代码已经遍历哈希表找到了文件描述符 fd 对应的迭代器位置。

    /*find() 查找以 key 为键的键值对，如果找到，则返回一个指向该键值对的正向迭代器；反之，则返回一个指向容器中最后一个键值对之后位置的迭代器（如果 end() 方法返回的迭代器）*/

    if(fd_it == fd2path_.end()){
        throw FileNotOpenError(fd);//文件未打开 ，设置错误码
    }

    path2fd_.erase(fd2path_[fd]);
    fd2path_.erase(fd_it);
    //直接通过已知的迭代器位置 fd_it 删除元素
    /*如果此时使用 erase(fd)：哈希表要再次重新计算 fd 的哈希值。再次遍历桶（bucket） 查找键 fd 的位置。重复的查找操作会导致 O(n) 的时间复杂度（最坏情况）。*/
    if(close(fd) == -1){
        throw InternalError("DiskManager::close_file: Close Error");
    }
   
}


/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_size(const std::string &file_name) {
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager::get_file_name(int fd) {
    if (!fd2path_.count(fd)) {
        throw FileNotOpenError(fd);
    }
    //count()方法返回值是一个整数，1表示有这个元素，0表示没有这个元素。
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager::get_file_fd(const std::string &file_name) {
    if (!path2fd_.count(file_name)) {
        return open_file(file_name);
    }
    return path2fd_[file_name];
}


/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager::read_log(char *log_data, int size, int offset) {
    // read log file from the previous end
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size) {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if(size == 0) return 0;
    lseek(log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(log_fd_, log_data, size);
    assert(bytes_read == size);
    /*assert 是一种用于调试程序的宏，主要用于在运行时检查一个条件是否为真。如果条件不满足，程序将终止执行并输出一条错误信息*/

    return bytes_read;
}


/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager::write_log(char *log_data, int size) {
    if (log_fd_ == -1) {
        log_fd_ = open_file(LOG_FILE_NAME);
    }

    // write from the file_end
    lseek(log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(log_fd_, log_data, size);
    if (bytes_write != size) {
        throw UnixError();
    }
}

/*
lseek()函数： 对于所有打开的文件都有一个当前文件偏移量(current file offset)，文件偏移量通常是一个非负整数，用于表明文件开始处到文件当前位置的字节数。读写操作通常开始于当前文件偏移量的位置，并且使其增大，增量为读写的字节数。文件被打开时，文件的偏移量会被初始化为 0，除非使用了O_APPEND 。
读写操作可以使文件的偏移量发生变化;而lseek 函数也可以改变文件的当前位置。下面我们一块来看一下关于lseek函数的原型及使用方法。
通过man手册查看到lseek函数原型如下：需要的头文件为
#include
#include
off_t lseek(int fd, off_t offset, int whence);
参数：fd：文件描述符
offset：文件偏移量
whence：文件偏移相对位置
返回值：成功：返回文件新的偏移量(成功)
失败：-1(失败)
参数 offset可正可负，负数时向文件开头偏移，正数相对于文件末尾偏移
参数 offset 的含义取决于参数 whence：
1. 如果 whence 是 SEEK_SET，offset相对于文件开头进行偏移
2. 如果 whence 是 SEEK_CUR，offset相对文件当前位置进行偏移
3. 如果 whence 是 SEEK_END，offset相对于文件末尾进行偏移
对于whence参数的选项SEEK_SET、SEEK_CUR 和 SEEK_END 是 System V 引入的，在这之前使用的是 0、1 和 2。*/