/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    if(file_handle->file_hdr_.num_pages > 1){  
    //如果文件包含多个页面 ，遍历页面找到第一个存放记录（num_records>0)的页面 
        for(int pageno =1 ;pageno < file_handle->file_hdr_.num_pages;pageno++){
            if(file_handle->fetch_page_handle(pageno).page_hdr->num_records > 0){
                int i = Bitmap::first_bit(1,file_handle->fetch_page_handle(pageno).bitmap,file_handle->file_hdr_.num_records_per_page);
                rid_.slot_no = i;
                rid_.page_no = pageno;
                return;
            }

        }
    }
    rid_ = Rid{-1,-1}; // 如果没找到返回无效值
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
   int maxPageNum =  file_handle_->file_hdr_.num_pages;
   int pageno = rid_.page_no;
   int slotno = rid_.slot_no;
        for(;pageno < maxPageNum;pageno++){ //遍历页面
            int i = Bitmap::next_bit(1,file_handle_->fetch_page_handle(pageno).bitmap,file_handle_->file_hdr_.num_records_per_page,slotno);
           //查找下一个包含记录的 插槽序号
            if(i == file_handle_->file_hdr_.num_records_per_page){
                //如果 i 等于 页面最大插槽数
                slotno = -1;//slotno 置 -1 从头再找？？
                continue;
            }
            else{
                rid_.page_no = pageno;
                rid_.slot_no = i;
                return;
            }
        }
   
   rid_ = Rid{-1,-1};
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    return rid_.page_no == RM_NO_PAGE;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}