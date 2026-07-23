"""Exporter — kéo dữ liệu vận hành từ hệ thống chạy về nơi phân tích được.

Chiều ngược với deployer: deployer đẩy desired state từ metadata LÊN runtime;
exporter kéo trạng thái thật từ runtime VỀ bảng query được (ClickHouse), cho BI.
Không sinh artifact, không phải nguồn sự thật — chỉ là bản chiếu để phân tích.
"""
