from django.test import SimpleTestCase
from infra.events.admin import mismatches, topic_value_matches


class RedpandaConfigContractTests(SimpleTestCase):
    def test_effective_cluster_disabled_satisfies_topic_false_only(self):
        self.assertTrue(topic_value_matches('write.caching', 'false', 'disabled'))
        self.assertTrue(topic_value_matches('write.caching', 'false', 'false'))
        self.assertFalse(topic_value_matches('write.caching', 'false', 'true'))
        self.assertFalse(topic_value_matches('write.caching', 'false', None))
        self.assertFalse(topic_value_matches('write.caching', 'disabled', 'false'))
        self.assertFalse(topic_value_matches('cleanup.policy', 'false', 'disabled'))

    def test_alias_does_not_hide_other_topic_or_replication_mismatches(self):
        wanted=[{'name':'inventory','partitions':1,'replication_factor':3,
                 'config':{'write.caching':'false','cleanup.policy':'delete'}}]
        observed={'inventory':{'partitions':[{'partition':0,'leader':0,'replicas':[0,1,2],'isr':[0,1,2]}],
                               'config':{'write.caching':{'value':'disabled'},'cleanup.policy':{'value':'delete'}}}}
        self.assertEqual(mismatches(wanted,observed,full_isr=True),[])
        observed['inventory']['config']['cleanup.policy']['value']='compact'
        self.assertEqual(len(mismatches(wanted,observed)),1)
        observed['inventory']['partitions'][0]['replicas']=[0]
        self.assertEqual(len(mismatches(wanted,observed)),2)
