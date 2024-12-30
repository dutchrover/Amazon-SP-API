from .base_client import BaseClient
import time

class ReportsAPI(BaseClient):
    def create_report(self, report_type, data_start_time=None, data_end_time=None, 
                     report_options=None, timezone="UTC"):
        """
        Create a report request with enhanced options
        
        Args:
            report_type (str): Type of report to generate
            data_start_time (str): ISO start date for report data
            data_end_time (str): ISO end date for report data
            report_options (dict): Additional report options
            timezone (str): Timezone for report data
        """
        endpoint = '/reports/2021-06-30/reports'
        body = {
            'reportType': report_type,
            'marketplaceIds': [self.config.MARKETPLACE_ID],
            'timezone': timezone
        }
        
        if data_start_time:
            body['dataStartTime'] = data_start_time
        if data_end_time:
            body['dataEndTime'] = data_end_time
        if report_options:
            body['reportOptions'] = report_options
            
        return self._make_request('POST', endpoint, data=body)

    def get_report(self, report_id):
        """Get report by ID"""
        endpoint = f'/reports/2021-06-30/reports/{report_id}'
        return self._make_request('GET', endpoint)

    def get_report_document(self, report_document_id):
        """Get report document by ID"""
        endpoint = f'/reports/2021-06-30/documents/{report_document_id}'
        return self._make_request('GET', endpoint)