package greeter

import GrpcAgent.PublishReply
import GrpcAgent.PublishRequest
import GrpcAgent.PublisherGrpcKt
import GrpcAgent.TopicReply
import GrpcAgent.publishReply
import GrpcAgent.publishRequest
import GrpcAgent.topicReply
import com.google.gson.Gson
import io.grpc.Server
import io.grpc.ServerBuilder
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

interface SetDelegate {
    fun set(re: PublishRequest)
}

interface GetDelegate {
    fun get(re: PublishRequest)
}

class HelloWorldServer(private val port: Int): SetDelegate, GetDelegate {
    val stack: MutableList<PublishRequest> = emptyList<PublishRequest>().toMutableList()

    val server: Server = ServerBuilder
        .forPort(port)
        .addService(PublishService(this, stack))
        .build()

    fun start() {
        stack.add(publishRequest { topic="h";content = "a1" })
        stack.add(publishRequest { topic="h";content = "a2" })
        stack.add(publishRequest { topic="h";content = "a3" })
        server.start()
        println("Server running on port $port")
        Runtime.getRuntime().addShutdownHook(
            Thread {
                println("**** Shutting down gRPC server since JVM is shutting down")
                server.shutdown()
                println("**** Server shut down")
            }
        )
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }

    override fun set(re: PublishRequest) {
        println("--------------")
        println("RE: $re")
        stack.add(re)

    }
    override fun get(re: PublishRequest) {
        println(re)
    }

    class PublishService(stack: SetDelegate, private val sss: MutableList<PublishRequest>): PublisherGrpcKt.PublisherCoroutineImplBase() {
        private val delegate: SetDelegate = stack

        override suspend fun publishMessage(request: PublishRequest) : PublishReply {
            println("Topic: " + request.topic + "  Content: " + request.content)
            when (request.topic) {
                "grpc-XML" -> {

                }
                "grpc-JSON" -> {
                    val gson = Gson()
                    val req = request.content.replace("Topic","topic_").replace("Content","content_")
                    val deserealizedRequest = gson.fromJson(req, PublishRequest::class.java)
                    delegate.set(deserealizedRequest)
                }
                else -> {
                    delegate.set(request)
                }
            }
            return publishReply { isSuccess = true.toString() }
    }
        override fun listenMessage(requests: Flow<PublishRequest>): Flow<TopicReply> {

            return flow {
                requests.collect{ request ->
                    val topic = request.topic
                    if (topic.isNotEmpty()) {
                        println("Requested: $topic")
                        val result = sss.filter { it.topic == topic }.map {
                            return@map topicReply{content = it.content }
                        }
                        if (result.isEmpty()){
                            emit(topicReply {
                                content = "There's no information about this topic!"
                            })
                        } else {
                            for(res in result) {
                                emit(res)
                            }
                        }
                    }

                }
            }

        }

    }
}

fun main() {
    val port = System.getenv("PORT")?.toInt() ?: 50051
    val server = HelloWorldServer(port)
    server.start()
    server.blockUntilShutdown()
}