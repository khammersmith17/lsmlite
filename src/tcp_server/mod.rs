use crate::constants;
use crate::database::Nldb;
use crate::parser::NldbRequest;
use crate::ser;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn run(db: Nldb) {
    let listener = TcpListener::bind(constants::TCP_ADDRESS)
        .await
        .expect("Unable to listen on port 7211");

    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::task::spawn(serve_connection(stream, db.clone()));
    }
}

async fn serve_connection(mut stream: TcpStream, db: Nldb) {
    let mut in_buffer = BytesMut::with_capacity(2 << 11);
    let mut out_buffer = BytesMut::with_capacity(2 << 11);
    loop {
        in_buffer.clear();
        let (n_read, n_sent) = read_message(&mut stream, &mut in_buffer).await;
        if n_read == 0 {
            return;
        }

        match NldbRequest::parse(in_buffer[..n_sent].to_vec()) {
            Ok(s) => handle_request(s, &db, &mut out_buffer).await,
            Err(e) => {
                ser::construct_error_message(e, &mut out_buffer);
            }
        };

        match stream.write_all(&out_buffer).await {
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

async fn handle_request(statement: NldbRequest, db: &Nldb, out_buffer: &mut BytesMut) {
    match statement {
        NldbRequest::Get { key } => {
            let query_result = db.get(&key).await;
            ser::serialize_get_response(query_result, out_buffer)
        }
        NldbRequest::Insert { key, value } => {
            db.write(key, value).await;
            ser::insert_response(out_buffer)
        }
        NldbRequest::Delete { key } => {
            db.delete(key).await;
            ser::delete_response(out_buffer);
        }
    };
}

async fn read_message(stream: &mut TcpStream, buffer: &mut BytesMut) -> (usize, usize) {
    let Ok(size) = stream.read_u32().await else {
        return (0_usize, 0_usize);
    };

    let size = size as usize;
    if size > constants::MAX_MESSAGE_SIZE {
        return (0, 0);
    }
    let mut bytes_read = 0_usize;

    loop {
        match stream.read_buf(buffer).await {
            Ok(0) => return (0_usize, 0_usize),
            Ok(n) => {
                bytes_read += n;
                if bytes_read >= size {
                    return (bytes_read, size);
                }
            }
            Err(_) => return (0_usize, 0_usize),
        }
    }
}
