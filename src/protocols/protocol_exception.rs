use crate::binary::Encoder;
use crate::error_codes::ErrorCodes;
use crate::errors::Error;
use crate::protocols::*;

pub struct ExceptionResponse {}

impl ExceptionResponse {
    pub fn write(encoder: &mut Encoder, error: &Error, with_stack_trace: bool) {
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
        encoder.string(message);
        // StackTrace
        encoder.string(stack_trace);
        // Nested.
        encoder.write(false);
    }
}
