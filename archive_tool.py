import os
import sys
import tarfile
from datetime import datetime


def print_usage():
    """显示使用帮助信息"""
    print("用法: python archive_tool.py [选项]")
    print("选项:")
    print("  -c, --compress   压缩指定文件夹和文件到归档文件")
    print("  -x, --extract    从指定归档文件解压到当前文件夹")
    print("  -h, --help       显示帮助信息")
    sys.exit(1)


def check_targets(targets):
    """检查目标是否存在，返回存在的目标列表"""
    existing = []
    missing = []
    for target in targets:
        if os.path.exists(target):
            existing.append(target)
        else:
            missing.append(target)
    
    # 显示警告信息
    for item in missing:
        print(f"警告: 目标 '{item}' 不存在，将跳过")
    
    # 如果所有目标都不存在，返回None
    if not existing:
        print("错误: 所有目标都不存在，无法执行压缩")
        return None
    return existing


def compress():
    """压缩指定目标到tar.gz文件"""
    # 定义需要处理的目标
    targets = ["pic", "mgz", ".env.prod", "groups.db"]
    existing_targets = check_targets(targets)
    
    if not existing_targets:
        sys.exit(1)
    
    # 生成带时间戳的压缩文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"backup_{timestamp}.tar.gz"
    
    try:
        # 创建压缩文件并添加目标
        with tarfile.open(archive_name, "w:gz") as tar:
            for target in existing_targets:
                # 添加目标到压缩文件，保留相对路径
                tar.add(target, arcname=os.path.basename(target))
        
        print(f"压缩成功，生成文件: {archive_name}")
        print(f"包含的内容: {', '.join(existing_targets)}")
    
    except Exception as e:
        print(f"压缩失败: {str(e)}")
        # 清理可能生成的不完整文件
        if os.path.exists(archive_name):
            os.remove(archive_name)
        sys.exit(1)


def extract(archive_file):
    """从指定压缩文件解压内容"""
    # 检查压缩文件是否存在
    if not os.path.isfile(archive_file):
        print(f"错误: 文件 '{archive_file}' 不存在")
        sys.exit(1)
    
    try:
        # 检查文件是否为有效的tar.gz文件
        if not tarfile.is_tarfile(archive_file):
            print(f"错误: '{archive_file}' 不是有效的tar归档文件")
            sys.exit(1)
        
        # 解压文件到当前目录
        with tarfile.open(archive_file, "r:gz") as tar:
            tar.extractall()
        
        print(f"解压成功，文件已提取到当前文件夹")
    
    except tarfile.TarError as e:
        print(f"解压失败 (tar错误): {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"解压失败: {str(e)}")
        sys.exit(1)


def main():
    """主函数，解析命令行参数并执行相应操作"""
    if len(sys.argv) < 2:
        print_usage()
    
    arg = sys.argv[1]
    if arg in ("-h", "--help"):
        print_usage()
    elif arg in ("-c", "--compress"):
        compress()
    elif arg in ("-x", "--extract"):
        if len(sys.argv) < 3:
            print("请指定要解压的归档文件")
            print("示例: python archive_tool.py -x backup.tar.gz")
            sys.exit(1)
        extract(sys.argv[2])
    else:
        print(f"错误: 未知选项 '{arg}'")
        print_usage()


if __name__ == "__main__":
    main()