use std::io::Read;

use crate::binary::ReadEx;
use crate::errors::Result;

use super::*;

const TCP: u8 = 1;
const HTTP: u8 = 2;

#[derive(Default, Debug)]
pub struct QueryClientInfo {
    pub query_kind: u8,
    pub initial_user: String,
    pub initial_query_id: String,

    pub initial_address: String,
    pub interface: u8,

    // TCP
    pub os_user: String,
    pub client_hostname: String,
    pub client_name: String,

    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    pub client_revision: u64,

    // HTTP
    pub http_method: u8,
    pub http_user_agent: String,

    pub quota_key: String,
}

impl QueryClientInfo {
    pub fn read_from<R: Read>(
        reader: &mut R,
    ) -> Result<QueryClientInfo> {
        let mut client_info = QueryClientInfo {
            query_kind: reader.read_scalar()?,
            ..Default::default()
        };

        if client_info.query_kind == 0 {
            return Ok(client_info);
        }

        client_info.initial_user = reader.read_string()?;
        client_info.initial_query_id = reader.read_string()?;
        client_info.initial_address = reader.read_string()?;
        client_info.interface = reader.read_scalar()?;

        match client_info.interface {
            TCP => {
                client_info.os_user = reader.read_string()?;
                client_info.client_hostname = reader.read_string()?;
                client_info.client_name = reader.read_string()?;

                client_info.client_version_major = reader.read_uvarint()?;
                client_info.client_version_minor = reader.read_uvarint()?;
                let client_revision = reader.read_uvarint()?;

                client_info.client_revision = client_revision;
                client_info.client_version_patch = client_revision;
            }
            HTTP => {
                client_info.http_method = reader.read_scalar()?;
                client_info.http_user_agent = reader.read_string()?;
            }
            _ => {}
        }

        if client_info.client_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
            client_info.quota_key = reader.read_string()?;
        }

        if client_info.interface == TCP
            && client_info.client_revision >= DBMS_MIN_REVISION_WITH_VERSION_PATCH
        {
            client_info.client_version_patch = reader.read_uvarint()?;
        }

        // TODO
        // if client_info.client_revision >= DBMS_MIN_REVISION_WITH_OPENTELEMETRY {
        //     let trace_id: u8 = reader.read_scalar()?;
        //     if trace_id > 0 {
        //     }
        // }

        Ok(client_info)
    }
}

#[derive(Default, Debug)]
pub struct QueryRequest {
    pub(crate) query_id: String,
    pub(crate) client_info: QueryClientInfo,
    pub(crate) stage: u64,
    pub(crate) compression: u64,
    pub(crate) query: String,
}

impl QueryRequest {
    pub fn read_from<R: Read>(
        reader: &mut R,
        hello_request: &HelloRequest,
    ) -> Result<QueryRequest> {
        let query_id = reader.read_string()?;

        let mut client_info = Default::default();
        if hello_request.client_revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO {
            client_info = QueryClientInfo::read_from(reader)?;
        }

        if client_info.query_kind == 0 {
            client_info.query_kind = INITIAL_QUERY;
            client_info.client_name = hello_request.client_name.clone();
            client_info.client_version_major = hello_request.client_version_major;
            client_info.client_version_minor = hello_request.client_version_minor;
            client_info.client_version_patch = hello_request.client_version_patch;
            client_info.client_revision = hello_request.client_revision;
        }

        client_info.interface = TCP;

        // TODO after setUser
        // if client_info.query_kind == INITIAL_QUERY {
        //     /// 'Current' fields was set at receiveHello.
        //     client_info.initial_user = hello_request.current_user;
        //     client_info.initial_query_id = client_info.current_query_id;
        //     client_info.initial_address = client_info.current_address;
        // }
        //

        // TODO: all settings
        loop {
            let str = reader.read_string()?;
            if str.is_empty() {
                break;
            }
        }

        let query_protocol = QueryRequest {
            query_id,
            client_info,
            stage: reader.read_uvarint()?,
            compression: reader.read_uvarint()?,
            query: reader.read_string()?,
        };

        Ok(query_protocol)
    }
}
