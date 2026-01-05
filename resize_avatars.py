#!/usr/bin/env python3
"""
批量缩小已存在的头像图片到 128x128
"""

import sys
from pathlib import Path
from PIL import Image
from tqdm import tqdm

# 导入 avatar_dir
avatar_dir = Path("imgs") / "avatar"


def resize_avatars():
    """缩小 avatar_dir 中所有的 PNG 图片到 128x128"""
    
    if not avatar_dir.exists():
        print(f"错误：头像目录不存在 {avatar_dir}")
        return
    
    # 获取所有 PNG 文件
    png_files = list(avatar_dir.glob("*.png"))
    
    if not png_files:
        print(f"未找到任何 PNG 文件在 {avatar_dir}")
        return
    
    print(f"找到 {len(png_files)} 个头像文件，开始处理...")
    
    success_count = 0
    error_count = 0
    
    for png_file in tqdm(png_files, desc="处理进度"):
        try:
            # 打开图片
            img = Image.open(png_file)
            
            # 记录原始尺寸
            original_size = img.size
            
            if original_size == (128, 128):
                # 已经是 128x128，跳过
                continue
            # 缩小到 128x128
            img = img.resize((128, 128), Image.Resampling.LANCZOS)
            
            # 保存回原文件
            img.save(png_file, "PNG")
            
            success_count += 1
            
        except Exception as e:
            print(f"\n处理失败: {png_file} - {e}")
            error_count += 1
    
    print(f"\n完成！成功: {success_count}, 失败: {error_count}")


if __name__ == "__main__":
    resize_avatars()
