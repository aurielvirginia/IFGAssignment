import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.util.KeywordUtil
import internal.GlobalVariable as GlobalVariable


def correlationId = UUID.randomUUID().toString()


def request = findTestObject('Object Repository/API/POST_TriggerEvent', [('correlationId'): correlationId])

def response = WS.sendRequest(request)
WS.verifyResponseStatusCode(response, 200)

String topic = GlobalVariable.kafkaTopic
String bootstrap = GlobalVariable.kafkaBootstrapServers
String groupId = "katalon-consumer-" + System.currentTimeMillis()

String msg = CustomKeywords.'com.company.kafka.KafkaClient.consumeUntilContains'(bootstrap, topic, groupId, correlationId, 30)

KeywordUtil.markFailedAndStop("Kafka message not found for correlationId=${correlationId}")

assert msg.contains(correlationId)
KeywordUtil.logInfo("Kafka message payload: \n" + msg)