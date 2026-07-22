"""Verifier — đối chiếu contract với nguồn sự thật độc lập, không chỉ với artifact.

Khác `cli.py check` ở chỗ căn bản:
  - `check` hỏi: "artifact trên đĩa có khớp metadata không?" (metadata là chuẩn).
  - verifier hỏi: "metadata có khớp hiện thực không?" (database/registry là chuẩn).

Cái sau bịt một lỗ hổng cái trước không thấy: nếu `columns` trong contract lệch với
schema Postgres thật, mọi artifact sinh ra đều thừa hưởng lỗi đó — và `check` vẫn
xanh, vì nó chỉ so bản sinh với chính contract sai. Chỉ có đối chiếu với nguồn độc
lập mới bắt được.
"""
