import aws_cdk as core
import aws_cdk.assertions as assertions

from analytiikka_talous_hankinta.analytiikka_talous_hankinta_stack import AnalytiikkaTalousHankintaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in analytiikka_talous_hankinta/analytiikka_talous_hankinta_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AnalytiikkaTalousHankintaStack(app, "analytiikka-talous-hankinta")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
