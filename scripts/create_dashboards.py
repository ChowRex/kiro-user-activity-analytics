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
        """创建行为分析数据集（JOIN user_mapping 获取用户名）"""
        ds_arn = f"arn:aws:quicksight:{self.config['aws']['region']}:{self.account_id}:datasource/{data_source_id}"
        db = self.config['glue']['database_name']
        physical = {
            'activity': {
                'RelationalTable': {
                    'DataSourceArn': ds_arn, 'Catalog': 'AwsDataCatalog',
                    'Schema': db, 'Name': 'by_user_analytic',
                    'InputColumns': [
                        {'Name': 'userid', 'Type': 'STRING'},
                        {'Name': 'date', 'Type': 'STRING'},
                        {'Name': 'chat_aicodelines', 'Type': 'STRING'},
                        {'Name': 'chat_messagesinteracted', 'Type': 'STRING'},
                        {'Name': 'chat_messagessent', 'Type': 'STRING'},
                        {'Name': 'codefix_acceptanceeventcount', 'Type': 'STRING'},
                        {'Name': 'codefix_acceptedlines', 'Type': 'STRING'},
                        {'Name': 'codefix_generatedlines', 'Type': 'STRING'},
                        {'Name': 'codefix_generationeventcount', 'Type': 'STRING'},
                        {'Name': 'codereview_failedeventcount', 'Type': 'STRING'},
                        {'Name': 'codereview_findingscount', 'Type': 'STRING'},
                        {'Name': 'codereview_succeededeventcount', 'Type': 'STRING'},
                        {'Name': 'dev_acceptanceeventcount', 'Type': 'STRING'},
                        {'Name': 'dev_acceptedlines', 'Type': 'STRING'},
                        {'Name': 'dev_generatedlines', 'Type': 'STRING'},
                        {'Name': 'dev_generationeventcount', 'Type': 'STRING'},
                        {'Name': 'docgeneration_acceptedfileupdates', 'Type': 'STRING'},
                        {'Name': 'docgeneration_acceptedfilescreations', 'Type': 'STRING'},
                        {'Name': 'docgeneration_acceptedlineadditions', 'Type': 'STRING'},
                        {'Name': 'docgeneration_acceptedlineupdates', 'Type': 'STRING'},
                        {'Name': 'docgeneration_eventcount', 'Type': 'STRING'},
                        {'Name': 'docgeneration_rejectedfilecreations', 'Type': 'STRING'},
                        {'Name': 'docgeneration_rejectedfileupdates', 'Type': 'STRING'},
                        {'Name': 'docgeneration_rejectedlineadditions', 'Type': 'STRING'},
                        {'Name': 'docgeneration_rejectedlineupdates', 'Type': 'STRING'},
                        {'Name': 'inlinechat_acceptanceeventcount', 'Type': 'STRING'},
                        {'Name': 'inlinechat_acceptedlineadditions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_acceptedlinedeletions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_dismissaleventcount', 'Type': 'STRING'},
                        {'Name': 'inlinechat_dismissedlineadditions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_dismissedlinedeletions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_rejectedlineadditions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_rejectedlinedeletions', 'Type': 'STRING'},
                        {'Name': 'inlinechat_rejectioneventcount', 'Type': 'STRING'},
                        {'Name': 'inlinechat_totaleventcount', 'Type': 'STRING'},
                        {'Name': 'inline_aicodelines', 'Type': 'STRING'},
                        {'Name': 'inline_acceptancecount', 'Type': 'STRING'},
                        {'Name': 'inline_suggestionscount', 'Type': 'STRING'},
                        {'Name': 'testgeneration_acceptedlines', 'Type': 'STRING'},
                        {'Name': 'testgeneration_acceptedtests', 'Type': 'STRING'},
                        {'Name': 'testgeneration_eventcount', 'Type': 'STRING'},
                        {'Name': 'testgeneration_generatedlines', 'Type': 'STRING'},
                        {'Name': 'testgeneration_generatedtests', 'Type': 'STRING'},
                        {'Name': 'transformation_eventcount', 'Type': 'STRING'},
                        {'Name': 'transformation_linesgenerated', 'Type': 'STRING'},
                        {'Name': 'transformation_linesingested', 'Type': 'STRING'},
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
        int_cols = [
            'chat_aicodelines', 'chat_messagesinteracted', 'chat_messagessent',
            'codefix_acceptanceeventcount', 'codereview_findingscount',
            'codereview_succeededeventcount',
            'dev_acceptanceeventcount', 'dev_generatedlines', 'dev_generationeventcount',
            'docgeneration_eventcount', 'docgeneration_acceptedfilescreations',
            'inlinechat_totaleventcount', 'inlinechat_acceptanceeventcount',
            'inline_aicodelines', 'inline_acceptancecount', 'inline_suggestionscount',
            'testgeneration_eventcount', 'testgeneration_acceptedtests',
            'transformation_eventcount', 'transformation_linesgenerated',
        ]
        cast_transforms = [{'CastColumnTypeOperation': {
            'ColumnName': c, 'NewColumnType': 'INTEGER'
        }} for c in int_cols]
        logical = {
            'activity-base': {
                'Alias': 'activity_data',
                'Source': {'PhysicalTableId': 'activity'},
                'DataTransforms': cast_transforms,
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
        return 'kiro-user-activity-dataset'

    def create_credits_dataset(self, data_source_id):
        """创建 Credits 数据集（JOIN user_mapping 获取用户名）"""
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
                        {'Name': 'chat_conversations', 'Type': 'STRING'},
                        {'Name': 'credits_used', 'Type': 'STRING'},
                        {'Name': 'overage_cap', 'Type': 'STRING'},
                        {'Name': 'overage_credits_used', 'Type': 'STRING'},
                        {'Name': 'overage_enabled', 'Type': 'STRING'},
                        {'Name': 'profileid', 'Type': 'STRING'},
                        {'Name': 'subscription_tier', 'Type': 'STRING'},
                        {'Name': 'total_messages', 'Type': 'STRING'},
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
        credits_cast = [
            {'CastColumnTypeOperation': {'ColumnName': 'total_messages', 'NewColumnType': 'INTEGER'}},
            {'CastColumnTypeOperation': {'ColumnName': 'chat_conversations', 'NewColumnType': 'INTEGER'}},
            {'CastColumnTypeOperation': {'ColumnName': 'credits_used', 'NewColumnType': 'DECIMAL'}},
            {'CastColumnTypeOperation': {'ColumnName': 'overage_cap', 'NewColumnType': 'DECIMAL'}},
            {'CastColumnTypeOperation': {'ColumnName': 'overage_credits_used', 'NewColumnType': 'DECIMAL'}},
        ]
        logical = {
            'credits-base': {
                'Alias': 'credits_data',
                'Source': {'PhysicalTableId': 'credits'},
                'DataTransforms': credits_cast,
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
        return 'kiro-user-credits-dataset'
    
    
    
    def deploy_all(self):
        """部署所有资源"""
        print("开始部署 QuickSight 资源...\n")

        # 1. 创建数据源
        data_source_id = self.create_data_source()

        # 2. 创建数据集
        dataset_id = self.create_dataset(data_source_id)
        credits_dataset_id = self.create_credits_dataset(data_source_id)

        print("\n✓ 数据源和数据集部署完成！")
        print(f"\n访问 QuickSight 控制台查看: https://{self.config['aws']['region']}.quicksight.aws.amazon.com/")

if __name__ == '__main__':
    deployer = QuickSightDeployer()
    deployer.deploy_all()
