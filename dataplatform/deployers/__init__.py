"""Deployer — áp artifact sinh từ metadata lên engine đang chạy, idempotent.

Generator sinh ra "trạng thái mong muốn" (desired state). Deployer đưa hệ thống
thật về đúng trạng thái đó, và làm được nhiều lần mà không gây hại (idempotent).

Ranh giới với generator: generator không chạm hệ thống chạy (chỉ sinh file);
deployer không quyết định nội dung (chỉ áp cái generator sinh). Hai việc tách bạch
để mỗi cái kiểm chứng riêng được.
"""
