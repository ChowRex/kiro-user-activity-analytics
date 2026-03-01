#!/usr/bin/env python3
import boto3
import yaml
import json
from pathlib import Path

class QuickSightDeployer:
    def __init__(self, config_path='config.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.qs = boto3.client('quicksight', region_name=self.config['aws']['region'])
        self.account_id = self.config['aws']['account_id']
        
    def create_data_source(self):
        """创建 Athena 数据源"""
        try:
            response = self.qs.create_data_source(
                AwsAccountId=self.account_id,
                DataSourceId='kiro-athena-datasource',
                Name=self.config['quicksight']['data_source_name'],
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'kiro-analytics-workgroup'
                    }
                },
                Permissions=[{
                    'Principal': self.config['quicksight']['user_arn'],
                    'Actions': [
                        'quicksight:DescribeDataSource',
                        'quicksight:DescribeDataSourcePermissions',
                        'quicksight:PassDataSource',
                        'quicksight:UpdateDataSource',
                        'quicksight:DeleteDataSource',
                        'quicksight:UpdateDataSourcePermissions'
                    ]
                }]
            )
            print(f"✓ 数据源创建成功: {response['DataSourceId']}")
            return response['DataSourceId']
        except self.qs.exceptions.ResourceExistsException:
            print("✓ 数据源已存在")
            return 'kiro-athena-datasource'
    
    def create_dataset(self, data_source_id):
        """创建行为分析数据集（JOIN user_mapping 获取用户名，SPICE 模式）"""
        ds_arn = f"arn:aws:quicksight:{self.config['aws']['region']}:{self.account_id}:datasource/{data_source_id}"
        db = self.config['glue']['database_name']
        physical = {
            'activity': {
                'RelationalTable': {
                    'DataSourceArn': ds_arn, 'Catalog': 'AwsDataCatalog',
                    'Schema': db, 'Name': 'by_user_analytic',
                    'InputColumns': [
                        {'Name': 'date', 'Type': 'STRING'},
                        {'Name': 'userid', 'Type': 'STRING'},
                        {'Name': 'chat_aicodelines', 'Type': 'INTEGER'},
                        {'Name': 'chat_messagesinteracted', 'Type': 'INTEGER'},
                        {'Name': 'chat_messagessent', 'Type': 'INTEGER'},
                        {'Name': 'inline_aicodelines', 'Type': 'INTEGER'},
                        {'Name': 'inline_acceptancecount', 'Type': 'INTEGER'},
                        {'Name': 'inline_suggestionscount', 'Type': 'INTEGER'},
                        {'Name': 'codefix_generationeventcount', 'Type': 'INTEGER'},
                        {'Name': 'codefix_acceptanceeventcount', 'Type': 'INTEGER'},
                        {'Name': 'codereview_findingscount', 'Type': 'INTEGER'},
                        {'Name': 'codereview_succeededeventcount', 'Type': 'INTEGER'},
                        {'Name': 'dev_generationeventcount', 'Type': 'INTEGER'},
                        {'Name': 'dev_acceptanceeventcount', 'Type': 'INTEGER'},
                        {'Name': 'dev_generatedlines', 'Type': 'INTEGER'},
                        {'Name': 'testgeneration_eventcount', 'Type': 'INTEGER'},
                        {'Name': 'testgeneration_acceptedtests', 'Type': 'INTEGER'},
                        {'Name': 'inlinechat_totaleventcount', 'Type': 'INTEGER'},
                        {'Name': 'inlinechat_acceptanceeventcount', 'Type': 'INTEGER'},
                        {'Name': 'docgeneration_eventcount', 'Type': 'INTEGER'},
                        {'Name': 'docgeneration_acceptedfilescreations', 'Type': 'INTEGER'},
                        {'Name': 'transformation_eventcount', 'Type': 'INTEGER'},
                        {'Name': 'transformation_linesgenerated', 'Type': 'INTEGER'},
                    ]
                }
            },
            'mapping': {
                'RelationalTable': {
                    'DataSourceArn': ds_arn, 'Catalog': 'AwsDataCatalog',
                    'Schema': db, 'Name': 'user_mapping',
                    'InputColumns': [
                        {'Name': 'userid', 'Type': 'STRING'},
                        {'Name': 'username', 'Type': 'STRING'},
                    ]
                }
            }
        }
        logical = {
            'activity-base': {
                'Alias': 'activity_data',
                'Source': {'PhysicalTableId': 'activity'},
            },
            'mapping-base': {
                'Alias': 'user_mapping',
                'Source': {'PhysicalTableId': 'mapping'},
                'DataTransforms': [{
                    'RenameColumnOperation': {
                        'ColumnName': 'userid',
                        'NewColumnName': 'map_userid'
                    }
                }]
            },
            'activity-joined': {
                'Alias': 'activity_with_username',
                'Source': {
                    'JoinInstruction': {
                        'LeftOperand': 'activity-base',
                        'RightOperand': 'mapping-base',
                        'Type': 'LEFT',
                        'OnClause': 'userid = map_userid'
                    }
                },
                'DataTransforms': [{
                    'ProjectOperation': {
                        'ProjectedColumns': [
                            'date', 'userid', 'username',
                            'chat_aicodelines', 'chat_messagesinteracted', 'chat_messagessent',
                            'inline_aicodelines', 'inline_acceptancecount', 'inline_suggestionscount',
                            'codefix_generationeventcount', 'codefix_acceptanceeventcount',
                            'codereview_findingscount', 'codereview_succeededeventcount',
                            'dev_generationeventcount', 'dev_acceptanceeventcount', 'dev_generatedlines',
                            'testgeneration_eventcount', 'testgeneration_acceptedtests',
                            'inlinechat_totaleventcount', 'inlinechat_acceptanceeventcount',
                            'docgeneration_eventcount', 'docgeneration_acceptedfilescreations',
                            'transformation_eventcount', 'transformation_linesgenerated',
                        ]
                    }
                }]
            }
        }
        params = dict(
            AwsAccountId=self.account_id,
            DataSetId='kiro-user-activity-dataset',
            Name=self.config['quicksight']['dataset_name'],
            PhysicalTableMap=physical,
            LogicalTableMap=logical,
            ImportMode='SPICE',
            Permissions=[{
                'Principal': self.config['quicksight']['user_arn'],
                'Actions': [
                    'quicksight:DescribeDataSet', 'quicksight:DescribeDataSetPermissions',
                    'quicksight:PassDataSet', 'quicksight:DescribeIngestion',
                    'quicksight:ListIngestions', 'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet', 'quicksight:CreateIngestion',
                    'quicksight:CancelIngestion', 'quicksight:UpdateDataSetPermissions'
                ]
            }]
        )
        try:
            self.qs.create_data_set(**params)
            print(f"✓ 行为分析数据集创建成功 (SPICE 模式)")
        except self.qs.exceptions.ResourceExistsException:
            del params['Permissions']
            self.qs.update_data_set(**params)
            print(f"✓ 行为分析数据集已更新 (SPICE 模式)")
        
        # 触发首次 SPICE 导入
        self._trigger_ingestion('kiro-user-activity-dataset')
        # 设置每日刷新计划
        self._create_refresh_schedule('kiro-user-activity-dataset', 'activity-daily-refresh')
        return 'kiro-user-activity-dataset'

    def create_credits_dataset(self, data_source_id):
        """创建 Credits 数据集（JOIN user_mapping 获取用户名，SPICE 模式）"""
        ds_arn = f"arn:aws:quicksight:{self.config['aws']['region']}:{self.account_id}:datasource/{data_source_id}"
        db = self.config['glue']['database_name']
        physical = {
            'credits': {
                'RelationalTable': {
                    'DataSourceArn': ds_arn, 'Catalog': 'AwsDataCatalog',
                    'Schema': db, 'Name': 'user_report',
                    'InputColumns': [
                        {'Name': 'date', 'Type': 'STRING'},
                        {'Name': 'userid', 'Type': 'STRING'},
                        {'Name': 'client_type', 'Type': 'STRING'},
                        {'Name': 'subscription_tier', 'Type': 'STRING'},
                        {'Name': 'total_messages', 'Type': 'INTEGER'},
                        {'Name': 'chat_conversations', 'Type': 'INTEGER'},
                        {'Name': 'credits_used', 'Type': 'DECIMAL'},
                        {'Name': 'overage_cap', 'Type': 'DECIMAL'},
                        {'Name': 'overage_credits_used', 'Type': 'DECIMAL'},
                        {'Name': 'overage_enabled', 'Type': 'STRING'},
                        {'Name': 'profileid', 'Type': 'STRING'},
                    ]
                }
            },
            'mapping2': {
                'RelationalTable': {
                    'DataSourceArn': ds_arn, 'Catalog': 'AwsDataCatalog',
                    'Schema': db, 'Name': 'user_mapping',
                    'InputColumns': [
                        {'Name': 'userid', 'Type': 'STRING'},
                        {'Name': 'username', 'Type': 'STRING'},
                    ]
                }
            }
        }
        logical = {
            'credits-base': {
                'Alias': 'credits_data',
                'Source': {'PhysicalTableId': 'credits'},
            },
            'mapping2-base': {
                'Alias': 'user_mapping2',
                'Source': {'PhysicalTableId': 'mapping2'},
                'DataTransforms': [{
                    'RenameColumnOperation': {
                        'ColumnName': 'userid',
                        'NewColumnName': 'map_userid'
                    }
                }]
            },
            'credits-joined': {
                'Alias': 'credits_with_username',
                'Source': {
                    'JoinInstruction': {
                        'LeftOperand': 'credits-base',
                        'RightOperand': 'mapping2-base',
                        'Type': 'LEFT',
                        'OnClause': 'userid = map_userid'
                    }
                },
                'DataTransforms': [{
                    'ProjectOperation': {
                        'ProjectedColumns': [
                            'date', 'userid', 'username',
                            'client_type', 'subscription_tier',
                            'total_messages', 'chat_conversations',
                            'credits_used', 'overage_cap',
                            'overage_credits_used', 'overage_enabled', 'profileid',
                        ]
                    }
                }]
            }
        }
        params = dict(
            AwsAccountId=self.account_id,
            DataSetId='kiro-user-credits-dataset',
            Name='KiroUserCreditsDataset',
            PhysicalTableMap=physical,
            LogicalTableMap=logical,
            ImportMode='SPICE',
            Permissions=[{
                'Principal': self.config['quicksight']['user_arn'],
                'Actions': [
                    'quicksight:DescribeDataSet', 'quicksight:DescribeDataSetPermissions',
                    'quicksight:PassDataSet', 'quicksight:DescribeIngestion',
                    'quicksight:ListIngestions', 'quicksight:UpdateDataSet',
                    'quicksight:DeleteDataSet', 'quicksight:CreateIngestion',
                    'quicksight:CancelIngestion', 'quicksight:UpdateDataSetPermissions'
                ]
            }]
        )
        try:
            self.qs.create_data_set(**params)
            print(f"✓ Credits 数据集创建成功 (SPICE 模式)")
        except self.qs.exceptions.ResourceExistsException:
            del params['Permissions']
            self.qs.update_data_set(**params)
            print(f"✓ Credits 数据集已更新 (SPICE 模式)")
        
        # 触发首次 SPICE 导入
        self._trigger_ingestion('kiro-user-credits-dataset')
        # 设置每日刷新计划
        self._create_refresh_schedule('kiro-user-credits-dataset', 'credits-daily-refresh')
        return 'kiro-user-credits-dataset'

    def _trigger_ingestion(self, dataset_id):
        """触发 SPICE 数据导入"""
        import time
        ingestion_id = f"initial-{int(time.time())}"
        try:
            self.qs.create_ingestion(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id,
                IngestionId=ingestion_id
            )
            print(f"  ✓ SPICE 导入已触发: {dataset_id}")
        except Exception as e:
            print(f"  ⚠ SPICE 导入触发跳过: {e}")

    def _create_refresh_schedule(self, dataset_id, schedule_id):
        """创建每日 SPICE 刷新计划（UTC 4:00，在 user_mapping 同步之后）"""
        schedule = {
            'ScheduleId': schedule_id,
            'ScheduleFrequency': {
                'Interval': 'DAILY',
                'TimeOfTheDay': '04:00',
            },
            'RefreshType': 'FULL_REFRESH',
        }
        try:
            self.qs.create_refresh_schedule(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id,
                Schedule=schedule
            )
            print(f"  ✓ 每日刷新计划已创建: {schedule_id} (UTC 04:00)")
        except Exception as e:
            if 'already exists' in str(e).lower() or 'ResourceExists' in str(e):
                try:
                    self.qs.update_refresh_schedule(
                        AwsAccountId=self.account_id,
                        DataSetId=dataset_id,
                        Schedule=schedule
                    )
                    print(f"  ✓ 每日刷新计划已更新: {schedule_id} (UTC 04:00)")
                except Exception as e2:
                    print(f"  ⚠ 刷新计划更新跳过: {e2}")
            else:
                print(f"  ⚠ 刷新计划创建跳过: {e}")

    def deploy_all(self):
        """部署所有资源"""
        print("开始部署 QuickSight 资源...\n")

        # 1. 创建数据源
        data_source_id = self.create_data_source()

        # 2. 创建数据集 (SPICE 模式)
        dataset_id = self.create_dataset(data_source_id)
        credits_dataset_id = self.create_credits_dataset(data_source_id)

        print("\n✓ 数据源和数据集部署完成 (SPICE 模式，每日自动刷新)")
        print(f"\n访问 QuickSight 控制台查看: https://{self.config['aws']['region']}.quicksight.aws.amazon.com/")

if __name__ == '__main__':
    deployer = QuickSightDeployer()
    deployer.deploy_all()
