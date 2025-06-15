import sys
import os
import msal
import glob
import time
from office365.graph_client import GraphClient
from office365.runtime.odata.v4.upload_session_request import UploadSessionRequest
from office365.onedrive.driveitems.driveItem import DriveItem
from office365.onedrive.internal.paths.url import UrlPath
from office365.runtime.queries.upload_session import UploadSessionQuery
from office365.onedrive.driveitems.uploadable_properties import DriveItemUploadableProperties

# --------------------------
# 🧾 参数提取
# --------------------------
site_name = sys.argv[1]
sharepoint_host_name = sys.argv[2]
tenant_id = sys.argv[3]
client_id = sys.argv[4]
client_secret = sys.argv[5]
upload_path = sys.argv[6]
file_path = sys.argv[7]

try:
    max_retry = max(1, int(sys.argv[8]))
except:
    max_retry = 3

login_endpoint = sys.argv[9] if len(sys.argv) > 9 else "login.microsoftonline.com"
graph_endpoint = sys.argv[10] if len(sys.argv) > 10 else "graph.microsoft.com"

tenant_url = f'https://{sharepoint_host_name}/sites/{site_name}'

# --------------------------
# 🔐 获取 Microsoft Graph Token
# --------------------------
def acquire_token():
    authority_url = f'https://{login_endpoint}/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=client_id,
        client_credential=client_secret
    )
    token = app.acquire_token_for_client(scopes=[f"https://{graph_endpoint}/.default"])
    return token

# --------------------------
# 🔄 替换 Graph 端点（用于国家云）
# --------------------------
def rewrite_endpoint(request):
    request.url = request.url.replace("https://graph.microsoft.com", f"https://{graph_endpoint}")

# --------------------------
# 🔗 初始化 Graph Client
# --------------------------
client = GraphClient(acquire_token)
client.before_execute(rewrite_endpoint, False)
drive = client.sites.get_by_url(tenant_url).drive.root

# --------------------------
# 📦 收集本地待上传文件
# --------------------------
if "/**/" in file_path or file_path.endswith("/**") or file_path.endswith("/**/*"):
    local_files = glob.glob(file_path, recursive=True)
else:
    local_files = glob.glob(file_path)

if not local_files:
    print(f"[Error] No files matched pattern: {file_path}")
    sys.exit(1)

# 过滤掉目录，仅保留文件
local_files = [f for f in local_files if os.path.isfile(f)]
common_root = os.path.commonpath(local_files)

# --------------------------
# 📊 上传进度回调
# --------------------------
def progress_status(offset, file_size):
    print(f"Uploaded {offset} / {file_size} bytes ({offset / file_size * 100:.2f}%)")

def success_callback(remote_file):
    print(f"[✓] File uploaded to: {remote_file.web_url}")

# --------------------------
# 📤 分块上传（大文件）
# --------------------------
def resumable_upload(drive_folder, local_path, file_size, chunk_size, max_chunk_retry, timeout_secs):
    def _start_upload():
        with open(local_path, "rb") as local_file:
            session_request = UploadSessionRequest(
                local_file,
                chunk_size,
                lambda offset: progress_status(offset, file_size)
            )
            retry_seconds = timeout_secs / max_chunk_retry
            for session_request._range_data in session_request._read_next():
                for retry_number in range(max_chunk_retry):
                    try:
                        super(UploadSessionRequest, session_request).execute_query(qry)
                        break
                    except Exception as e:
                        if retry_number + 1 >= max_chunk_retry:
                            raise e
                        print(f"Retry {retry_number + 1}/{max_chunk_retry}: {e}")
                        time.sleep(retry_seconds)

    file_name = os.path.basename(local_path)
    return_type = DriveItem(drive_folder.context, UrlPath(file_name, drive_folder.resource_path))
    qry = UploadSessionQuery(return_type, {"item": DriveItemUploadableProperties(name=file_name)})
    drive_folder.context.add_query(qry).after_query_execute(_start_upload)
    return_type.get().execute_query()
    success_callback(return_type)

# --------------------------
# 📥 单个文件上传（自动选择小/大文件）
# --------------------------
def upload_file(drive_folder, local_path, chunk_size):
    file_size = os.path.getsize(local_path)
    if file_size < chunk_size:
        remote_file = drive_folder.upload_file(local_path).execute_query()
        success_callback(remote_file)
    else:
        resumable_upload(
            drive_folder,
            local_path,
            file_size,
            chunk_size,
            max_chunk_retry=60,
            timeout_secs=600
        )

# --------------------------
# 🚀 主上传逻辑（递归结构保持）
# --------------------------
for local_file in local_files:
    rel_path = os.path.relpath(local_file, start=common_root)
    remote_path = os.path.join(upload_path, rel_path).replace("\\", "/")

    # 获取目标文件夹对象（递归建目录）
    upload_target = drive.get_by_path(os.path.dirname(remote_path))

    for attempt in range(max_retry):
        try:
            upload_file(upload_target, local_file, chunk_size=4 * 1024 * 1024)
            break
        except Exception as e:
            print(f"[Error] Attempt {attempt + 1} failed: {e}")
            if attempt + 1 == max_retry:
                raise e
            time.sleep(3)
