import yaml


class Config():
    def __init__(self, filename):
        self.profile = self.load(filename)

    def load(self, filename):
        with open(filename, 'r') as f:
            data = yaml.safe_load(f)
        
        default_profile = data.get('default_profile', None)
        profiles = data.get('profiles', {})
        if default_profile in profiles:
            result = profiles[default_profile]
        elif len(profiles.keys()) > 0:
            result = profiles[profiles.keys()[0]]
        else:
            raise Exception("No profile")
        
        return result

    @property
    def bootstrap_servers(self):
        return self.profile['bootstrap_servers']

    @property
    def security_protocol(self):
        return self.profile['security_protocol']

    @property
    def sasl_mechanism(self):
        return self.profile['sasl_mechanism']

    @property
    def sasl_plain_username(self):
        return self.profile['sasl_plain_username']

    @property
    def sasl_plain_password(self):
        return self.profile['sasl_plain_password']