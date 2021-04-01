use std::io::Read;

use crate::errors::Result;
use crate::protocols::*;
use crate::{binary::Encoder, error_codes::ErrorCodes, errors::Error};

pub struct ExceptionResponse {}

impl ExceptionResponse {
    pub fn encode(encoder: &mut Encoder, error: &Error, with_stack_trace: bool) -> Result<()> {
        let mut code = ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT;
        let mut stack_trace = "".to_string();
        let mut message = error.to_string();

        if let Error::Server(e) = error {
            code = e.code;
            if with_stack_trace {
                stack_trace = e.stack_trace.clone();
            }
            message = e.message.clone();
        }
        encoder.uvarint(SERVER_EXCEPTION);

        encoder.write(code.bits());
        //Name
        encoder.string("");
        // Message
        encoder.string(error.to_string());
        // StackTrace
        encoder.string(stack_trace);
        // Nested.
        encoder.write(false);
        Ok(())
    }
}

// // Nested.
// if err := writer.Bool(false); err != nil {
//     return errors.Wrapf(err, "couldn't write nested")
// }
// return nil
