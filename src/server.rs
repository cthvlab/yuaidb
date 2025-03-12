use tonic::{Request, Response, Status};
use tonic::transport::Server;
use std::net::SocketAddr;
use sha2::{Sha256, Digest};
use crate::{Database, Row, Condition, OrderBy, GroupBy, Aggregate};
use crate::database::{DatabaseService, DatabaseServiceServer, InsertRequest, InsertResponse, UpdateRequest, UpdateResponse, DeleteRequest, DeleteResponse, SelectRequest, SelectResponse, MultiJoinRequest, MultiJoinResponse};

#[derive(Clone)]
struct DbService {
    db: Database,
    users: HashMap<String, String>,
}

#[tonic::async_trait]
impl DatabaseService for DbService {
    async fn insert(&self, request: Request<InsertRequest>) -> Result<Response<InsertResponse>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        self.db.insert(&req.table, Row {
            id: req.row.unwrap().id,
            data: req.row.unwrap().data,
        }).await;
        Ok(Response::new(InsertResponse { status: "Inserted".to_string() }))
    }

    async fn update(&self, request: Request<UpdateRequest>) -> Result<Response<UpdateResponse>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        let conditions = req.conditions.into_iter().map(|c| match c.condition_type {
            Some(database::condition::ConditionType::Eq(eq)) => Condition::Eq(eq.field, eq.value),
            Some(database::condition::ConditionType::Lt(lt)) => Condition::Lt(lt.field, lt.value),
            Some(database::condition::ConditionType::Gt(gt)) => Condition::Gt(gt.field, gt.value),
            Some(database::condition::ConditionType::Contains(cont)) => Condition::Contains(cont.field, cont.value),
            None => Condition::Eq("".to_string(), "".to_string()),
        }).collect();
        self.db.update(&req.table, conditions, req.updates).await;
        Ok(Response::new(UpdateResponse { status: "Updated".to_string() }))
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        let conditions = req.conditions.into_iter().map(|c| match c.condition_type {
            Some(database::condition::ConditionType::Eq(eq)) => Condition::Eq(eq.field, eq.value),
            Some(database::condition::ConditionType::Lt(lt)) => Condition::Lt(lt.field, lt.value),
            Some(database::condition::ConditionType::Gt(gt)) => Condition::Gt(gt.field, gt.value),
            Some(database::condition::ConditionType::Contains(cont)) => Condition::Contains(cont.field, cont.value),
            None => Condition::Eq("".to_string(), "".to_string()),
        }).collect();
        self.db.delete(&req.table, conditions).await;
        Ok(Response::new(DeleteResponse { status: "Deleted".to_string() }))
    }

    async fn select(&self, request: Request<SelectRequest>) -> Result<Response<SelectResponse>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        let conditions = req.conditions.into_iter().map(|c| match c.condition_type {
            Some(database::condition::ConditionType::Eq(eq)) => Condition::Eq(eq.field, eq.value),
            Some(database::condition::ConditionType::Lt(lt)) => Condition::Lt(lt.field, lt.value),
            Some(database::condition::ConditionType::Gt(gt)) => Condition::Gt(gt.field, gt.value),
            Some(database::condition::ConditionType::Contains(cont)) => Condition::Contains(cont.field, cont.value),
            None => Condition::Eq("".to_string(), "".to_string()),
        }).collect();
        let order_by = req.order_by.map(|o| OrderBy { field: o.field, ascending: o.ascending });
        let group_by = req.group_by.map(|g| GroupBy {
            field: g.field,
            aggregate: match g.aggregate.unwrap().aggregate_type {
                Some(database::aggregate::AggregateType::Count(_)) => Aggregate::Count,
                Some(database::aggregate::AggregateType::SumField(f)) => Aggregate::Sum(f),
                None => Aggregate::Count,
            },
        });
        let rows = self.db.select(&req.table, conditions, order_by, group_by).await;
        Ok(Response::new(SelectResponse { rows }))
    }

    async fn multi_join(&self, request: Request<MultiJoinRequest>) -> Result<Response<MultiJoinResponse>, Status> {
        self.authenticate(&request)?;
        let req = request.into_inner();
        let joins = req.joins.into_iter().map(|j| (j.table1.as_str(), j.field1.as_str(), j.table2.as_str(), j.field2.as_str())).collect();
        let row_sets = self.db.multi_join(joins).await;
        let row_sets = row_sets.into_iter().map(|rows| database::RowSet { rows }).collect();
        Ok(Response::new(MultiJoinResponse { row_sets }))
    }
}

impl DbService {
    fn authenticate<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let metadata = request.metadata();
        let login = metadata.get("login").ok_or(Status::unauthenticated("Login required"))?.to_str().map_err(|_| Status::unauthenticated("Invalid login"))?;
        let password = metadata.get("password").ok_or(Status::unauthenticated("Password required"))?.to_str().map_err(|_| Status::unauthenticated("Invalid password"))?;

        let stored_hash = self.users.get(login).ok_or(Status::unauthenticated("Invalid login"))?;
        let mut hasher = Sha256::new();
        hasher.update(password);
        let password_hash = format!("{:x}", hasher.finalize());

        if stored_hash == &password_hash {
            Ok(())
        } else {
            Err(Status::unauthenticated("Invalid password"))
        }
    }
}

pub async fn run_server(db: Database, addr: SocketAddr) {
    let mut users = HashMap::new();
    let mut hasher = Sha256::new();
    hasher.update("password123");
    users.insert("admin".to_string(), format!("{:x}", hasher.finalize()));

    let service = DbService { db, users };
    let server = Server::builder()
        .add_service(DatabaseServiceServer::new(service))
        .serve(addr);

    println!("gRPC server running on {}", addr);
    if let Err(e) = server.await {
        eprintln!("Server error: {}", e);
    }
}