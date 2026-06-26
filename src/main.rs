use std::net::SocketAddr;
use std::sync::Arc;
use std::{convert::Infallible};
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Bytes, service::service_fn};
use hyper_tungstenite::{is_upgrade_request, upgrade};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tungstenite::Message;
use futures_util::{SinkExt, StreamExt};

/*
    Struct for websocket communication between threads - used for registery of addresses
*/
enum Enveloppe {
    PartnerAdress(mpsc::Sender<Enveloppe>),
    Game(Message),
}

type Outbox = mpsc::Sender<Enveloppe>;
type WaitingSlot = Arc<Mutex<Option<Outbox>>>;


async fn handle_main(_req: &Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {

    Ok(Response::new(Full::new(Bytes::from("Main endpoint\n"))))
}


async fn handle_users(_req: &Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible>{

    Ok(Response::new(Full::new(Bytes::from("Users endpoint\n"))))
}


/* Http handler function, routes requests to relevant function */
async fn http_handler(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible>{

    match (req.method(), req.uri().path()) {

        // / route    
        (&Method::GET, "/") => {
            handle_main(&req).await
        }

        // /users route
        (&Method::GET, "/users") => {
            handle_users(&req).await
        }

        // everything else - return 404
        _ => {
            let mut not_found = Response::new(Full::new(Bytes::from("404 endpoint")));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

/*
    Managing websocket connections, pairing two consequent websocket connection and transmitting messages between
    Considered to be the prototype for exchanging moves

    Bugs:
     - we need to decide who is black and who is white and let them know

     - when one of the clients disconnects, the other should be informed about this (throw an error or smth)
       with a closing handshake or smth
           When closing a connection from client, we should send a special message and with this we'll also disconnect from other client too

     - We need to deal when a user is closed the connection before someone else is paired up with them
*/
async fn ws_handler(req: Request<hyper::body::Incoming>, waiting_slot: WaitingSlot) -> Result<Response<Full<Bytes>>, Infallible>{

    let (response, websocket) = match upgrade(req, None) {

        Ok(pair) => pair,

        Err(e) => {

            println!("Upgrade error in ws_handler: {e}");
            let mut bad_request = Response::new(Full::new(Bytes::from("400 bad request")));
            *bad_request.status_mut() = StatusCode::BAD_REQUEST;
            return Ok(bad_request);

        }
    };

    // spawn a thread to handle the websocket connection
    tokio::spawn( async move {

        match websocket.await {
            Ok(ws) => {
                let (mut sink, mut source ) = ws.split(); // split the ws

                // my channel variables
                let (my_tx, mut my_rx): (Outbox, mpsc::Receiver<Enveloppe>) = mpsc::channel(32);

                // check the list
                let already_partner_tx: Option<Outbox> = {
                    println!("slot is locking now");
                    let mut slot = waiting_slot.lock().await;
                    println!("slot is locked");

                    match slot.take() {
                        // found, return others tx
                        Some(found_partner_tx) => Some(found_partner_tx),

                        // not found, put yourself and wait
                        None => {

                            *slot = Some(my_tx.clone()); // put yourself
                            println!("we put myself and now will sleep");
                            None // return none, we'll handle that later
                            
                        }
                    }
                };
                let partner_tx = match already_partner_tx {
                    Some(already_partner_tx) => already_partner_tx,

                    None => {
                        match my_rx.recv().await { // wait until recieve - someone sends something

                            Some(Enveloppe::PartnerAdress(address)) => {let _ = address.send(Enveloppe::Game(Message::text("Youre black (opponent)"))).await;
                            address}, // when someone entered return its address to slot ?
                            _ => {
                                println!("Error unexpected");
                                return;
                            }
                        }
                    }
                };

                println!("we found someone now giving my address");

                // send to partner my address
                let _ = partner_tx.send(Enveloppe::PartnerAdress(my_tx.clone())).await;
                
                println!("paired done");

                let _ = my_tx.send(Enveloppe::Game(Message::text("GameStarting"))).await;
                let _ = my_tx.send(Enveloppe::Game(Message::text("I'm white"))).await;
                //let _ = partner_tx.send(Enveloppe::Game(Message::text("GameStarting"))).await;

                loop {

                    tokio::select! {

                        // reading part - read from source and send it to others tx - I'm sending branch
                        incoming = source.next() => { // get the next from source
                            match incoming {
    
                                Some(Ok(message)) => {
                                    if partner_tx.send(Enveloppe::Game(message)).await.is_err() {
                                        println!("Connection closed, breaking");
                                        break;
                                    }
                                }
                                Some(Err(e)) => {
                                    println!("error when reading message: {e}");
                                    break;
                                }
    
                                None => {
                                    println!("Client probably disconnected, breaking");
                                    break;
                                }
                            }
                        }
    
                        // the message from other, read it and send to me
                        enveloppe = my_rx.recv() => {
    
                            match enveloppe {
                                Some(Enveloppe::Game(message)) => {
                                    if sink.send(message).await.is_err() {
                                        println!("Failed to send to client, breaking");
                                        break;
                                    }
                                }
    
                                Some(Enveloppe::PartnerAdress(_)) => {
                                    // duplicate handshake, if we came over here, we should've been already matched up
                                }
    
                                None => {
                                    println!("partner channel closed");
                                    break;
                                }
    
                            }
                        }
                    }
                }
                
            },

            Err(e) => {
                println!("Error awaiting websocket : {e}");
            }
        }
        
    });

    // send the response immediatly, tokio's spawn will work in other thread
    Ok(response) 
}


/* Upper routing for websocket requests and http requests */
async fn upper_branch(req: Request<hyper::body::Incoming>, waiting_slot: WaitingSlot) -> Result<Response<Full<Bytes>>, Infallible> {
    
    if is_upgrade_request(&req) {
        println!("routing to ws_handler");
        ws_handler(req, waiting_slot).await
    } else {
        http_handler(req).await
    }
}

/*
    When user clicks on "play game" button, we will hold in a pool-like waiting list. When another user also clicks on "play game" button
    they'll get matched up immediatly (We'll consider smart matching algorithms way later). After, they'll be randomly assigned their colors. 
    As the logic in frontend tells, white will make the first move. That move should be sent over to server via ws connection (already implemented
    in ws_handler). The receiving end also should make the move automatically and set the turn to black 

     - Put first user to pool, when second user comes match them (already implemented by ws_handler ?)
*/

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> { // 

    let socket_addr = SocketAddr::from(([127, 0, 0, 1], 4000));

    let listener = TcpListener::bind(socket_addr).await?;

    let waiting_slot: WaitingSlot = Arc::new(Mutex::new(None));


    loop {
        let (stream, _addr) = listener.accept().await?;

        let io = TokioIo::new(stream);
        let waiting_slot_for_this_conn = waiting_slot.clone();

        tokio::task::spawn(
            async move {

                println!("ok, request came");
                let service = service_fn(move |req| {
                    upper_branch(req, waiting_slot_for_this_conn.clone())
                });

                if let Err(err) = hyper::server::conn::http1::Builder::new().serve_connection(io, service).with_upgrades().await {
                    eprintln!("Error serving connection {:?}", err);
                }
            }
        );
    }
}

