package greeter

import GrpcAgent.PublisherGrpcKt
import GrpcAgent.publishRequest
import com.google.protobuf.stringValue
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import java.io.Closeable
import java.util.concurrent.TimeUnit

class HelloWorldClient(channel: ManagedChannel) {
    private val stub = PublisherGrpcKt.PublisherCoroutineStub(channel)

    suspend fun greet(topic: String, content: String) {
        val request = publishRequest { this.topic = topic; this.content = content }
        try {
            val response = stub.publishMessage(request)
            println("Received response: ${response.allFields}")
        } catch (e: Exception){
            println("ERR "+ e.message)
        }

    }
}
class SubscriberClient(private val channel: ManagedChannel): Closeable {
    private val stub = PublisherGrpcKt.PublisherCoroutineStub(channel)
    private var stack: MutableList<String> = mutableListOf()
    suspend fun greet(topic: String) {
        val request = flow{emit(publishRequest { this.topic = topic })}
        val tempStack: MutableList<String> = mutableListOf()
        stub.listenMessage(request).collect{ value ->
                tempStack.add(value.content)
        }
        if (stack != tempStack) {
//            println("Start Proccesing")
            if(stack.lastIndex == -1) {
                tempStack.forEach { value ->
                    println("Content: $value")
                }
                stack = tempStack
            } else {
                val newStack = tempStack.subList(stack.lastIndex+1, tempStack.lastIndex+1)
//                println("Show new values:")
                newStack.forEach { value ->
                    println("Content: $value")
                    stack.add(value)
                }
            }
        }

    }

    override fun close() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }

}

suspend fun main2() {
//    val port = System.getenv("PORT")?.toInt() ?: 50051
//    val channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build()
    val channel = ManagedChannelBuilder.forTarget("4.tcp.eu.ngrok.io:16392").usePlaintext().build()
    val client = HelloWorldClient(channel)
    val topic = "hhh"
    println("Topic: $topic")
    val content = "b3"
    println("Content: $content")
    client.greet(topic, content)

}

suspend fun main() {
    val channel = ManagedChannelBuilder.forTarget("0.tcp.eu.ngrok.io:19784").usePlaintext().build()
    val client = SubscriberClient(channel)
    while (true) {
        client.use {
            while (true) {
                runBlocking {
                it.greet("123")
                }
                delay(5000)
            }
        }
    }
}