import os
import pandas as pd

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider

s3_key_id = os.environ.get('AWS_ACCESS_KEY')
s3_access_key = os.environ.get('AWS_SECRET_KEY')

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

HTTP = HTTPRemoteProvider()
FTP = FTPRemoteProvider()

contigs = [
    # Autosomes
   #'NC_009144_3','NC_009145_3','NC_009146_3', # chr1-3
   #'NC_009147_3','NC_009148_3','NC_009149_3', # chr4-6
   #'NC_009150_3','NC_009151_3','NC_009152_3', # chr7-9
   #'NC_009153_3','NC_009154_3','NC_009155_3', # chr10-12
   #'NC_009156_3','NC_009157_3','NC_009158_3', # chr13-15
   #'NC_009159_3','NC_009160_3','NC_009161_3', # chr16-18
   #'NC_009162_3','NC_009163_3','NC_009164_3', # chr19-21
   #'NC_009165_3','NC_009166_3','NC_009167_3', # chr22-24
   #'NC_009168_3','NC_009169_3','NC_009170_3', # chr25-27
   #'NC_009171_3','NC_009172_3','NC_009173_3', # chr28-30
    'NC_009174_3','NC_009175_3',               # chr31-32                         
   #'NC_001640_1',                             # Mitochindria
   #'unplaced'                                 # Unplaced/Chrunk
] 
caller = [
#    'gatk',
    'bcftools'
]
maf = [
    'MAF005',
    'MAF01'
]

lsts = [
    'MNEc2M', 
    'MNEc670k', 
    'SNP70', 
    'SNP50'
]

rule all:
    input:
        # Create the SNP LSTs
        S3.remote(expand('mccue-lab/Ec3Genomes/data/lsts/{LST}.lst',LST=lsts)),
        # Create snps VCFs
        expand('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.vcf.gz',caller=caller,maf=maf,contig=contigs),
        # Phase VCFs
        expand('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.phased.vcf.gz',caller=caller,maf=maf,contig=contigs)

rule phase_vcf:
    input:
        snps = 'mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.vcf.gz'
    resources:
        phase_jobs = 1
    threads: 20
    output:
        phased = 'mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.phased.vcf.gz'
    params:
        prefix = 'mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.phased'
    shell: 
        '''
            java -Xmx55g -jar .local/src/beagle.jar gt={input.snps} out={params.prefix} impute=true nthreads={threads} window=0.2 overlap=0.01 
        '''


rule subset_filter_bi_allelic_SNP_joint_vcf:
    input:
        vcf = S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz'),
        idx = S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz.csi'),
        lst = S3.remote('mccue-lab/Ec3Genomes/data/lsts/{lst}.lst')
    output: 
        snps= S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.{lst}.vcf.gz')
    shell:
        '''
            .local/bin/bcftools view {input.vcf} -R {input.lst} -o {output.snps} -O z    
        '''

rule index_vcf:
    input:
        vcf = S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz')
    output:
        idx = S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz.csi')
    shell:
        '''
            .local/bin/bcftools index {input.vcf}
        '''

rule filter_feature_joint_vcf:
    input:
        gvcf = S3.remote('mccue-lab/ibiodatatransfer2019/joint_{caller}/{contig}.gvcf.gz')
    output: 
        snps = 'mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{maf}/{contig}.vcf.gz'
    params:
        max_mem='2G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        shell(f'''
            .local/bin/bcftools view -m2 -v snps,indels --min-ac 50 --min-af {min_af} {{input.gvcf}} -Ou | \
            .local/bin/bcftools norm -m+any -Ou | \
            .local/bin/bcftools sort -m {{params.max_mem}} -O z -o {{output.snps}} 
        ''')

rule make_snp_lsts:
    '''
    good for: LST = [MNEc2M, MNEc670k, SNP70, SNP50]
    '''
    input:
        snps = HTTP.remote('https://www.animalgenome.org/repository/pub/UMN2018.1003/{LST}.unique_remap.FINAL.csv.gz'),
        assembly = FTP.remote('ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/002/863/925/GCF_002863925.1_EquCab3.0/GCF_002863925.1_EquCab3.0_assembly_report.txt')
    output:
        lst = S3.remote('mccue-lab/Ec3Genomes/data/lsts/{LST}.lst')
        #lst = 'mccue-lab/Ec3Genomes/data/lsts/{LST}.lst'
    run:
        import pandas as pd
        # Read in the assembly mapping
        assembly = pd.read_table(input.assembly, comment='#', header=None) 
        chrmap =  {x:y for x,y in zip(assembly[0].values, assembly[6].values)}
        # I have NO idea why the HTTP remote returns a list ...
        df = pd.read_table(input.snps[0],sep=',')
        
        def map_chrom(x): 
            'maps the chromosome name in the beeson et al file to the Ec3 RefSeq file'
            if x in chrmap:
                return chrmap[x]
            elif x.startswith('chrUn_ref'):
                return x.replace('chrUn_ref|','').replace('|','')
            else:
                raise ValueError
        
        # Add the refeseq chromosomes names to each  

        df['CHR'] = [map_chrom(x) for x in df.EC3_chrom]
        df['POS'] = df.EC3_pos
            
        # Output the LSTs file
        df.loc[:,['CHR','POS']].to_csv(output.lst,index=False,header=None,sep='\t')
