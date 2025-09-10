with open("process_log.txt", "r+", encoding="utf-8") as f:
    lines = f.readlines()
    last_index = max(i for i, line in enumerate(lines) if line.startswith("✅ Đã tạo sql_output3/"))
    # chỉ giữ từ đầu tới dòng cuối cùng "✅ ..."
    f.seek(0)
    f.writelines(lines[:last_index + 1])
    f.truncate()
