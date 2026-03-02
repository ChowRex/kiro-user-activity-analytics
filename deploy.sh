#!/bin/bash
set -e

# 支持 --from-step N 从第 N 步开始执行
FROM_STEP=1
while [[ $# -gt 0 ]]; do
    case $1 in
        --from-step) FROM_STEP=$2; shift 2;;
        *) echo "用法: $0 [--from-step N]  (N=1~6)"; exit 1;;
    esac
done

echo "🚀 开始部署 Kiro User Activity Analytics"
if [ "$FROM_STEP" -gt 1 ]; then
    echo "  ⏩ 从第 ${FROM_STEP} 步开始"
fi
echo ""

# ============================================
# 前置检查
# ============================================
if [ ! -f "config.yaml" ]; then
    echo "❌ 配置文件不存在，请先复制 config.example.yaml 为 config.yaml 并填写配置"
    exit 1
fi

command -v aws >/dev/null 2>&1 || { echo "❌ 需要安装 AWS CLI"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "❌ 需要安装 Python3"; exit 1; }

# 读取配置
REGION=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['aws']['region'])")
ACCOUNT_ID=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['aws']['account_id'])")
BUCKET=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['s3']['bucket_name'])")
PREFIX=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['s3']['prefix'])")
IDENTITY_STORE_ID=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['identity_center']['identity_store_id'])")
STACK_NAME="kiro-analytics-stack"
WORKGROUP="kiro-analytics-workgroup"
GLUE_DB="kiro_analytics"
QS_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/service-role/aws-quicksight-service-role-v0"
QS_USER_ARN=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['quicksight']['user_arn'])")
AWS_PROFILE=${AWS_PROFILE:-default}

echo "📋 配置信息:"
echo "  Region:    $REGION"
echo "  Account:   $ACCOUNT_ID"
echo "  S3 Bucket: $BUCKET"
echo "  S3 Prefix: $PREFIX"
echo "  CLI Porfile: $AWS_PROFILE"
echo ""

# ============================================
# 1. 部署 CloudFormation
# ============================================
if [ "$FROM_STEP" -le 1 ]; then
echo "1️⃣  部署基础设施 (CloudFormation)..."

# 检查 stack 是否处于 ROLLBACK_COMPLETE 等不可更新状态，自动清理
STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name $STACK_NAME \
    --region $REGION \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "NOT_FOUND")

if [ "$STACK_STATUS" = "ROLLBACK_COMPLETE" ] || [ "$STACK_STATUS" = "DELETE_FAILED" ]; then
    echo "  ⚠️  Stack 处于 $STACK_STATUS 状态，自动删除后重建..."
    aws cloudformation delete-stack --stack-name $STACK_NAME --region $REGION
    aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME --region $REGION
    echo "  ✓ 旧 Stack 已删除"
fi

aws cloudformation deploy \
    --template-file infrastructure/cloudformation.yaml \
    --stack-name $STACK_NAME \
    --parameter-overrides \
        S3BucketName=$BUCKET \
        S3Prefix=$PREFIX \
        IdentityStoreId=$IDENTITY_STORE_ID \
    --capabilities CAPABILITY_IAM \
    --region $REGION \
    --no-fail-on-empty-changeset

echo "✓ CloudFormation 部署完成"
echo ""
fi # step 1

# ============================================
# 2. 配置 Lake Formation 权限
# ============================================

# Lake Formation 授权辅助函数
grant_lf() {
    local PRINCIPAL=$1
    local RESOURCE=$2
    local PERMS=$3
    local DESC=$4
    aws lakeformation grant-permissions \
        --principal "DataLakePrincipalIdentifier=$PRINCIPAL" \
        --resource "$RESOURCE" \
        --permissions $PERMS \
        --region $REGION 2>/dev/null && echo "  ✓ $DESC" || echo "  ✓ $DESC (已存在)"
}

grant_lf_all_tables() {
    local PRINCIPAL=$1
    local PERMS=$2
    local DESC=$3
    for TABLE in by_user_analytic user_report user_mapping; do
        grant_lf "$PRINCIPAL" \
            "{\"Table\":{\"DatabaseName\":\"$GLUE_DB\",\"Name\":\"$TABLE\"}}" \
            "$PERMS" \
            "$DESC ($TABLE)"
    done
}

if [ "$FROM_STEP" -le 2 ]; then
echo "2️⃣  配置 Lake Formation 数据库权限..."

CALLER_ARN=$(aws sts get-caller-identity --query 'Arn' --output text)

# 当前用户: 数据库权限（建表需要）
grant_lf "$CALLER_ARN" \
    "{\"Database\":{\"Name\":\"$GLUE_DB\"}}" \
    "CREATE_TABLE ALTER DESCRIBE" \
    "当前用户数据库权限"

# QuickSight: 数据库权限
grant_lf "$QS_ROLE_ARN" \
    "{\"Database\":{\"Name\":\"$GLUE_DB\"}}" \
    "DESCRIBE" \
    "QuickSight 数据库权限"

# QuickSight 用户 IAM 角色: 数据库权限
QS_IAM_ROLE=$(python3 -c "
arn = '$QS_USER_ARN'
parts = arn.split('/')
if len(parts) >= 3:
    print('arn:aws:iam::$ACCOUNT_ID:role/' + parts[-2])
else:
    print('')
")
if [ -n "$QS_IAM_ROLE" ]; then
    grant_lf "$QS_IAM_ROLE" \
        "{\"Database\":{\"Name\":\"$GLUE_DB\"}}" \
        "DESCRIBE" \
        "QuickSight 用户角色数据库权限"
fi

# IAMAllowedPrincipals: 数据库权限
grant_lf "IAM_ALLOWED_PRINCIPALS" \
    "{\"Database\":{\"Name\":\"$GLUE_DB\"}}" \
    "ALL" \
    "IAMAllowedPrincipals 数据库权限"

# Lambda: 数据库权限
LAMBDA_ROLE_FULL_ARN=$(aws lambda get-function-configuration \
    --function-name kiro-user-mapping-sync \
    --query 'Role' --output text --region $REGION 2>/dev/null || echo "")
if [ -n "$LAMBDA_ROLE_FULL_ARN" ] && [ "$LAMBDA_ROLE_FULL_ARN" != "None" ]; then
    grant_lf "$LAMBDA_ROLE_FULL_ARN" \
        "{\"Database\":{\"Name\":\"$GLUE_DB\"}}" \
        "CREATE_TABLE ALTER DESCRIBE" \
        "Lambda 数据库权限"
fi

echo "✓ Lake Formation 数据库权限配置完成"
echo ""
fi # step 2

# ============================================
# 3. 通过 Glue API 创建外部表（替代 Glue Crawler）
# ============================================
if [ "$FROM_STEP" -le 3 ]; then
echo "3️⃣  通过 Glue API 创建外部表..."

python3 -c "
import boto3, yaml, sys

config = yaml.safe_load(open('config.yaml'))
region = config['aws']['region']
account_id = config['aws']['account_id']
bucket = config['s3']['bucket_name']
prefix = config['s3']['prefix']
glue_db = config['glue']['database_name']

glue = boto3.client('glue', region_name=region)

CSV_SERDE = {
    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
    'Parameters': {'separatorChar': ',', 'quoteChar': '\"', 'escapeChar': '\\\\'}
}
INPUT_FMT = 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUT_FMT = 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'

def create_table(name, columns, s3_path):
    table_input = {
        'Name': name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_path,
            'InputFormat': INPUT_FMT,
            'OutputFormat': OUTPUT_FMT,
            'SerdeInfo': CSV_SERDE,
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'skip.header.line.count': '1', 'classification': 'csv'}
    }
    try:
        glue.create_table(DatabaseName=glue_db, TableInput=table_input)
        print(f'  ✓ {name} 表创建成功')
    except glue.exceptions.AlreadyExistsException:
        # 旧 Crawler 建的表可能带分区列，无法直接更新，先删后建
        try:
            glue.delete_table(DatabaseName=glue_db, Name=name)
            glue.create_table(DatabaseName=glue_db, TableInput=table_input)
            print(f'  ✓ {name} 表已重建（删除旧表并重新创建）')
        except Exception as e:
            print(f'  ✗ {name} 表重建失败: {e}')
            sys.exit(1)

# by_user_analytic 表 (按 CSV header 实际列顺序，OpenCSVSerde 全部为 string)
create_table('by_user_analytic', [
    {'Name': 'userid', 'Type': 'string'},
    {'Name': 'date', 'Type': 'string'},
    {'Name': 'chat_aicodelines', 'Type': 'string'},
    {'Name': 'chat_messagesinteracted', 'Type': 'string'},
    {'Name': 'chat_messagessent', 'Type': 'string'},
    {'Name': 'codefix_acceptanceeventcount', 'Type': 'string'},
    {'Name': 'codefix_acceptedlines', 'Type': 'string'},
    {'Name': 'codefix_generatedlines', 'Type': 'string'},
    {'Name': 'codefix_generationeventcount', 'Type': 'string'},
    {'Name': 'codereview_failedeventcount', 'Type': 'string'},
    {'Name': 'codereview_findingscount', 'Type': 'string'},
    {'Name': 'codereview_succeededeventcount', 'Type': 'string'},
    {'Name': 'dev_acceptanceeventcount', 'Type': 'string'},
    {'Name': 'dev_acceptedlines', 'Type': 'string'},
    {'Name': 'dev_generatedlines', 'Type': 'string'},
    {'Name': 'dev_generationeventcount', 'Type': 'string'},
    {'Name': 'docgeneration_acceptedfileupdates', 'Type': 'string'},
    {'Name': 'docgeneration_acceptedfilescreations', 'Type': 'string'},
    {'Name': 'docgeneration_acceptedlineadditions', 'Type': 'string'},
    {'Name': 'docgeneration_acceptedlineupdates', 'Type': 'string'},
    {'Name': 'docgeneration_eventcount', 'Type': 'string'},
    {'Name': 'docgeneration_rejectedfilecreations', 'Type': 'string'},
    {'Name': 'docgeneration_rejectedfileupdates', 'Type': 'string'},
    {'Name': 'docgeneration_rejectedlineadditions', 'Type': 'string'},
    {'Name': 'docgeneration_rejectedlineupdates', 'Type': 'string'},
    {'Name': 'inlinechat_acceptanceeventcount', 'Type': 'string'},
    {'Name': 'inlinechat_acceptedlineadditions', 'Type': 'string'},
    {'Name': 'inlinechat_acceptedlinedeletions', 'Type': 'string'},
    {'Name': 'inlinechat_dismissaleventcount', 'Type': 'string'},
    {'Name': 'inlinechat_dismissedlineadditions', 'Type': 'string'},
    {'Name': 'inlinechat_dismissedlinedeletions', 'Type': 'string'},
    {'Name': 'inlinechat_rejectedlineadditions', 'Type': 'string'},
    {'Name': 'inlinechat_rejectedlinedeletions', 'Type': 'string'},
    {'Name': 'inlinechat_rejectioneventcount', 'Type': 'string'},
    {'Name': 'inlinechat_totaleventcount', 'Type': 'string'},
    {'Name': 'inline_aicodelines', 'Type': 'string'},
    {'Name': 'inline_acceptancecount', 'Type': 'string'},
    {'Name': 'inline_suggestionscount', 'Type': 'string'},
    {'Name': 'testgeneration_acceptedlines', 'Type': 'string'},
    {'Name': 'testgeneration_acceptedtests', 'Type': 'string'},
    {'Name': 'testgeneration_eventcount', 'Type': 'string'},
    {'Name': 'testgeneration_generatedlines', 'Type': 'string'},
    {'Name': 'testgeneration_generatedtests', 'Type': 'string'},
    {'Name': 'transformation_eventcount', 'Type': 'string'},
    {'Name': 'transformation_linesgenerated', 'Type': 'string'},
    {'Name': 'transformation_linesingested', 'Type': 'string'},
], f's3://{bucket}/{prefix}AWSLogs/{account_id}/KiroLogs/by_user_analytic/')

# user_report 表 (按 CSV header 实际列顺序，OpenCSVSerde 全部为 string)
# CSV header: Date,UserId,Client_Type,Chat_Conversations,Credits_Used,Overage_Cap,Overage_Credits_Used,Overage_Enabled,ProfileId,Subscription_Tier,Total_Messages
create_table('user_report', [
    {'Name': 'date', 'Type': 'string'},
    {'Name': 'userid', 'Type': 'string'},
    {'Name': 'client_type', 'Type': 'string'},
    {'Name': 'chat_conversations', 'Type': 'string'},
    {'Name': 'credits_used', 'Type': 'string'},
    {'Name': 'overage_cap', 'Type': 'string'},
    {'Name': 'overage_credits_used', 'Type': 'string'},
    {'Name': 'overage_enabled', 'Type': 'string'},
    {'Name': 'profileid', 'Type': 'string'},
    {'Name': 'subscription_tier', 'Type': 'string'},
    {'Name': 'total_messages', 'Type': 'string'},
], f's3://{bucket}/{prefix}AWSLogs/{account_id}/KiroLogs/user_report/')
"

echo "✓ 外部表创建完成"

# 建表完成后，统一授权所有 principal 的表级别 Lake Formation 权限
echo "  配置 Lake Formation 表级别权限..."

CALLER_ARN=$(aws sts get-caller-identity --query 'Arn' --output text)
grant_lf_all_tables "$CALLER_ARN" "SELECT DESCRIBE ALTER" "当前用户查询权限"

grant_lf_all_tables "IAM_ALLOWED_PRINCIPALS" "ALL" "IAMAllowedPrincipals 表权限"

grant_lf_all_tables "$QS_ROLE_ARN" "SELECT DESCRIBE" "QuickSight 表权限"

QS_IAM_ROLE=$(python3 -c "
arn = '$QS_USER_ARN'
parts = arn.split('/')
if len(parts) >= 3:
    print('arn:aws:iam::$ACCOUNT_ID:role/' + parts[-2])
else:
    print('')
")
if [ -n "$QS_IAM_ROLE" ]; then
    grant_lf_all_tables "$QS_IAM_ROLE" "SELECT DESCRIBE" "QuickSight 用户角色表权限"
fi

LAMBDA_ROLE_FULL_ARN=$(aws lambda get-function-configuration \
    --function-name kiro-user-mapping-sync \
    --query 'Role' --output text --region $REGION 2>/dev/null || echo "")
if [ -n "$LAMBDA_ROLE_FULL_ARN" ] && [ "$LAMBDA_ROLE_FULL_ARN" != "None" ]; then
    grant_lf_all_tables "$LAMBDA_ROLE_FULL_ARN" "SELECT DESCRIBE ALTER" "Lambda 表权限"
fi

echo ""
fi # step 3

# ============================================
# 4. 验证 Athena 数据查询
# ============================================
if [ "$FROM_STEP" -le 4 ]; then
echo "4️⃣  验证 Athena 数据查询..."

python3 -c "
import boto3, time, sys
athena = boto3.client('athena', region_name='$REGION')
tables = ['by_user_analytic', 'user_report']
ok = True

for t in tables:
    r = athena.start_query_execution(
        QueryString=f'SELECT COUNT(*) FROM $GLUE_DB.{t}',
        WorkGroup='$WORKGROUP')
    qid = r['QueryExecutionId']
    while True:
        s = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status']['State']
        if s == 'SUCCEEDED':
            cnt = athena.get_query_results(QueryExecutionId=qid)['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
            print(f'  ✓ {t}: {cnt} 条记录')
            break
        elif s == 'FAILED':
            reason = athena.get_query_execution(QueryExecutionId=qid)['QueryExecution']['Status'].get('StateChangeReason','')
            print(f'  ✗ {t}: {reason}')
            ok = False
            break
        time.sleep(2)
if not ok:
    sys.exit(1)
"

echo "✓ 数据验证通过"
echo ""
fi # step 4

# ============================================
# 5. 同步用户映射 (Identity Center → S3 → Athena)
# ============================================
if [ "$FROM_STEP" -le 5 ]; then
echo "5️⃣  同步用户名映射..."
python3 scripts/sync_user_mapping.py

# user_mapping 表可能被 sync 脚本重建，需要补授 Lake Formation 权限
echo "  补授 user_mapping 表 Lake Formation 权限..."
for PERM_PAIR in \
    "IAM_ALLOWED_PRINCIPALS|ALL|IAMAllowedPrincipals" \
    "$(aws sts get-caller-identity --query 'Arn' --output text)|SELECT DESCRIBE ALTER|当前用户" \
    "$QS_ROLE_ARN|SELECT DESCRIBE|QuickSight"; do
    IFS='|' read -r P PERMS DESC <<< "$PERM_PAIR"
    grant_lf "$P" \
        "{\"Table\":{\"DatabaseName\":\"$GLUE_DB\",\"Name\":\"user_mapping\"}}" \
        "$PERMS" \
        "$DESC user_mapping 权限"
done
echo ""
fi # step 5

# ============================================
# 6. 部署 QuickSight 数据源、数据集和 Dashboard
# ============================================
if [ "$FROM_STEP" -le 6 ]; then
echo "6️⃣  部署 QuickSight 数据源和数据集 (SPICE 模式)..."
python3 scripts/create_datasets.py
echo ""

echo "7️⃣  发布 QuickSight Dashboard..."
python3 scripts/create_dashboard_publish.py
echo ""
fi # step 6

# ============================================
# 完成
# ============================================
echo "✅ 端到端部署完成！"
echo ""
echo "📊 访问 QuickSight 控制台查看仪表板:"
echo "   https://$REGION.quicksight.aws.amazon.com/"
echo ""
