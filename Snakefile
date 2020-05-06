import os
import sys
import pandas as pd

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider

#-----------------------------------------------------------------------------
#       Set up remotes here
#-----------------------------------------------------------------------------

S3 = S3RemoteProvider()

HTTP = HTTPRemoteProvider()
FTP = FTPRemoteProvider()
try:
    s3_key_id = os.environ['AWS_ACCESS_KEY']
    s3_access_key = os.environ['AWS_SECRET_KEY']
except KeyError as e:
    print("Export env variables 'AWS_ACCESS_KEY' and 'AWS_SECRET_KEY'")
    sys.exit(1) 

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

# Load the configuration file
configfile: "config.yaml"
# Now make sure the tmp dir in the config file is present
os.makedirs(config['tmp_dir'],exist_ok=True)

# Change the workdir so we dont spam the current directory with tons of I/O folders
workdir: "workdir"

rule all:
    input:
       ## Step 01 -- filter GVCFS to WGS vcfs
       #S3.remote(
       #    expand(
       #        '{bucket}/vcfs/{contig}/step01/{fltr}_{maf}_{caller}.WGS.vcf.gz',
       #        bucket=config['bucket'],  caller=config['caller'], 
       #        maf=config['maf'],        fltr=config['fltr'],       
       #        contig=config['contigs']
       #    )
       #),
       ## Step 02 -- Recalibrate scores for GATK/bcftools WGS VCFs
       #S3.remote(
       #    expand(
       #        '{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz',
       #        bucket=config['bucket'], 
       #        maf=config['maf'],        fltr=config['fltr'],       
       #        contig=config['contigs'], vqsr=config['vqsr']
       #    )
       #),
       ### Step 03 -- PHASE VCFs
       #S3.remote(
       #    expand(
       #        '{bucket}/vcfs/{contig}/step03/{fltr}_{maf}_COMBINED_{vqsr}.PHASED.WGS.vcf.gz',
       #        bucket=config['bucket'], 
       #        maf=config['maf'],        fltr=config['fltr'],       
       #        contig=config['contigs'], vqsr=config['vqsr']
       #    )
       #)
       ### Step 04 -- Filter and Impute
        S3.remote(
            expand(
                '{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_concord.diff.indv',
                bucket=config['bucket'],  sample=config['samples'], 
                maf=config['maf'],        fltr=config['fltr'],       
                contig=config['contigs'], vqsr=config['vqsr']
            ),
            keep_local=True
        )
   


#-----------------------------------------------------------------------------
#                           Step 00 - Helper Rules 
#-----------------------------------------------------------------------------

rule create_vcf_tbi:
    input:
        vcf   = S3.remote('{SLUG}.vcf.gz')
    output:
        index = S3.remote('{SLUG}.vcf.gz.tbi')
    shell:  
        '''
            gatk IndexFeatureFile -I {input.vcf} -O {output.index}
        '''
rule create_vcf_csi:
    input:
        vcf = S3.remote('{SLUG}.vcf.gz')
    output:
        idx = S3.remote('{SLUG}.vcf.gz.csi')
    shell:
        '''
            bcftools index {input.vcf} -o {output.idx}
        '''
rule move_ref_genome:
    input:
        S3.remote('mccue-lab/ibiodatatransfer2019/GCF_002863925.1_EquCab3.0_genomic/{fna}')
    output:
        S3.remote('{bucket}/fna/{fna}')
    shell:
        'cp {input[0]} {output[0]}'

rule download_fna:
    input:
        S3.remote('{bucket}/fna/{fna}')
    output:
        '{bucket}/fna/{fna}'
    shell:
        'cp {input[0]} {output[0]}'

rule move_ref_genome_dict:
    '''
        In GATK, you specify the FNA file and the program assumes the name of 
        the index files. So this rule just copies the index file we have into 
        the path expected by GATK based on its name. 
    '''
    input:
        fdict  = S3.remote('{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.fna.dict')
    output:
        fdict  = S3.remote('{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.dict')
    shell:
        'cp {input[0]} {output[0]}'

#-----------------------------------------------------------------------------
#       Step 01 - Filter GVCFs                          
#-----------------------------------------------------------------------------

rule filter_joint_vcf_biallelic:
    input:
        gvcf = S3.remote('mccue-lab/ibiodatatransfer2019/joint_bcftools/{contig}.gvcf.gz')
    output: 
        vcf = S3.remote('{bucket}/vcfs/{contig}/step01/biallelic_{maf}_{caller}.WGS.vcf.gz')
    resources:
        disk_gb = 100,
        mem_gb = 10
    params:
        max_mem = '10G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        tmpdir = f"{config['tmp_dir']}/{wildcards.contig}/biallelic_{wildcards.maf}_{wildcards.caller}/"
        shell(f'''
            mkdir -p {tmpdir}
            bcftools view -m2 -M2 -v snps,indels --min-af {min_af} {{input.gvcf}} -Ou | \
            bcftools norm -m+any -Ou | \
            bcftools sort -T {tmpdir} -m {{params.max_mem}} -O z -o {{output.vcf}} 
            rm -fr {tmpdir}
        ''')


#-----------------------------------------------------------------------------
#       Step 02 - Recalibrate VCFs (VQSLOD)           
#-----------------------------------------------------------------------------

# Step 2.1 - Download the LST files to use as gold standards in VQSLOD 
rule make_snp_lsts:
    '''
        This rule creates LST files for SNPS on each of the current (MNEc) 
        and past (SNP50/70) arrays. It uses the remapped SNPs from Beeson et al.

        Wildcard good for: LST = [MNEc2M, MNEc670k, SNP70, SNP50]
    '''
    input:
        # strins in parens get auto concatenated in Python
        snps = HTTP.remote(
            'https://www.animalgenome.org/repository/pub'
            '/UMN2018.1003/{LST}.unique_remap.FINAL.csv.gz'
        ),
        assembly = FTP.remote(
            'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/002/'
            '863/925/GCF_002863925.1_EquCab3.0/'
            'GCF_002863925.1_EquCab3.0_assembly_report.txt'
        )
    output:
        lst = S3.remote('{bucket}/lsts/{LST}.lst')
    run:
        import pandas as pd
        # Read in the assembly mapping for chromosome names 
        # (since they are NC_XXXXX here and chrXX in beeson et al.)
        assembly = pd.read_table(input.assembly ,comment='#', header=None) 
        chrmap =  {x:y for x,y in zip(assembly[0].values, assembly[6].values)}
        # NOTE: I have NO idea why the HTTP remote returns a list ...
        # this could be a bug and might get fixed in newer versions of SnakeMake
        df = pd.read_table(input.snps[0],sep=',')
        
        def map_chrom(x): 
            '''
                Maps the chromosome name in the Beeson et al. file to the Ec3 RefSeq file
            '''
            if x in chrmap:
                return chrmap[x]
            elif x.startswith('chrUn_ref'):
                return x.replace('chrUn_ref|','').replace('|','')
            else:
                raise ValueError('Could not map chromosome name')
        
        # Create a DF with two columns: chromosome and position 
        df['CHR'] = [map_chrom(x) for x in df.EC3_chrom]
        df['POS'] = df.EC3_pos
            
        # Output the LSTs file
        df.loc[:,['CHR','POS']].to_csv(output.lst,index=False,header=None,sep='\t')

# Step 2.2 - Filter FULL VCF to gold standard MNEc2M SNPs

rule filter_VCF_to_LST_SNPs:
    input:
        vcf = S3.remote('{bucket}/vcfs/{contig}/step01/{fltr}_{maf}_{caller}.WGS.vcf.gz'),
        idx = S3.remote('{bucket}/vcfs/{contig}/step01/{fltr}_{maf}_{caller}.WGS.vcf.gz.csi'),
        lst = S3.remote('{bucket}/lsts/{lst}.lst')
    output: 
        # This regex matches for 'callers' that are not COMBINED, which is covered by rule: combine_gatk_bcftools_vcfs
        # Plus 10 points to House Schaefer
        snps = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller,(?!COMBINED).+}.{lst}.vcf.gz')
    shell:
        '''
            bcftools view {input.vcf} -R {input.lst} -o {output.snps} -O z    
        '''
# Step 2.3 - Recalibrate VCFs using MNEc2M as gold standard LST
rule recalibrate_vcf:
    input:
        vcf       = S3.remote('{bucket}/vcfs/{contig}/step01/{flt}_{maf}_{caller}.WGS.vcf.gz'),
        vcfidx    = S3.remote('{bucket}/vcfs/{contig}/step01/{flt}_{maf}_{caller}.WGS.vcf.gz.tbi'),
        mnec2m    = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.MNEc2M.vcf.gz'),
        index     = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.MNEc2M.vcf.gz.tbi'),
        fna       = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai       = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict     = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    output:
        recal     = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal'),
        recalidx  = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal.idx'),
        plots     = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal.plots'),
        plotspdf  = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal.plots.pdf'),
        tranches  = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal.tranches'),
        tranchpdf = S3.remote('{bucket}/vcfs/{contig}/step02/{flt}_{maf}_{caller}.recal.tranches.pdf'),
    run:
        # Generate the recal flags from whats in the file
        import re
        import gzip
        info_fields = set()
        info_pattern = re.compile(r'''\#\#INFO=<
            ID=(?P<id>[^,]+),\s*
            Number=(?P<number>-?\d+|\.|[AGR])?,\s*
            Type=(?P<type>Integer|Float|Flag|Character|String),\s*
            Description="(?P<desc>[^"]*)"
            (?:,\s*Source="(?P<source>[^"]*)")?
            (?:,\s*Version="?(?P<version>[^"]*)"?)?
            >''', re.VERBOSE)
        with gzip.open(input.mnec2m,'rt') as IN:
            for line in IN:
                if not line.startswith('#'):
                    break
                elif line.startswith('##INFO'):
                    ID = re.match(info_pattern,line).group(1)
                    info_fields.add(ID)
                else:
                    continue
        # Define low variance fields per contig -- these will be removed from the target features
        # below
        low_var_fields = {
            'NC_009149_3' : set(['RPB'])
        }
        # bcftools target fields
        # flags = '-an DP -an RPB -an MQB -an BQB -an MQSB -an SGB'
        # gatk target fields
        # flags = '-an DP -an QD -an MQ -an ReadPosRankSum -an FS -an SOR'
        target_fields = ({
            'gatk' : set(['DP','QD','MQ','ReadPosRankSum','FS','SOR','ICB','RPB']),
            'bcftools' : set(['DP','RPB','MQB','BQB','MQSB','SGB'])
        }[wildcards.caller]).difference(
            low_var_fields.get(wildcards.contig,set())
        )

        flags = ' '.join(f'-an {x}' for x in info_fields.intersection(target_fields))

        shell(f'''
            gatk VariantRecalibrator \
            -R {{input.fna}} \
            -V {{input.vcf}} \
            --resource:MNEc2M,known=false,training=true,truth=true,prior=15.0 {{input.mnec2m}} \
            {flags} \
            -mode SNP \
            --max-gaussians 4 \
            -tranche 100.0 -tranche 99.9 -tranche 99.5 -tranche 99.0 -tranche 95.0 \
            -O {{output.recal}} \
            --tranches-file {{output.tranches}} \
            --rscript-file  {{output.plots}}
        ''')

rule apply_VQSR_filters_vcf:
    input:
        # VCF 
        vcf    = S3.remote('{bucket}/vcfs/{contig}/step01/{fltr}_{maf}_{caller}.WGS.vcf.gz'),
        vcfidx = S3.remote('{bucket}/vcfs/{contig}/step01/{fltr}_{maf}_{caller}.WGS.vcf.gz.tbi'),
        # Recalibration input
        recal     = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal'),
        recalidx  = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal.idx'),
        plots     = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal.plots'),
        plotspdf  = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal.plots.pdf'),
        tranches  = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal.tranches'),
        tranchpdf = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller}.recal.tranches.pdf'),
        # Refgen
        fna    = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai    = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict  = '{bucket}/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    output:
        vcf = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{caller,(?!COMBINED).+}_{vqsr}.WGS.vcf.gz'),
    run:
        import tempfile
        vqsr = int(wildcards.vqsr.replace('VQSR',''))
        if vqsr > 100:
            vqsr = vqsr / 10
        else:
            vqsr = vqsr
        tmp_vcf = tempfile.NamedTemporaryFile(dir=config['tmp_dir'])
        shell(f'''
            gatk ApplyVQSR \
            -R {{input.fna}} \
            -V {{input.vcf}} \
            --truth-sensitivity-filter-level {vqsr} \
            --tranches-file {{input.tranches}} \
            --recal-file {{input.recal}} \
            -mode SNP \
            -O {tmp_vcf.name} 
            bcftools view -f PASS {tmp_vcf.name} -Ou |\
            bcftools annotate -x ^INFO/VQSLOD,^FORMAT/GT -O z -o {{output.vcf}}
        ''')

rule combine_gatk_bcftools_vcfs:
    input:
        gatk_vcf     = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_gatk_{vqsr}.WGS.vcf.gz'),
        gatk_idx     = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_gatk_{vqsr}.WGS.vcf.gz.tbi'),
        bcftools_vcf = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_bcftools_{vqsr}.WGS.vcf.gz'),
        bcftools_idx = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_bcftools_{vqsr}.WGS.vcf.gz.tbi'),
        fna          = '{bucket}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai          = '{bucket}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict        = '{bucket}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    output:
        merged_bare                = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{vqsr}_MERGED_BARE.vcf.gz'),
        merged_partial_annotated   = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_{vqsr}_MERGED_PARTIAL_ANNOTATED.vcf.gz'),
        merged_fully_annotated     = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz'),
        merged_fully_annotated_idx = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz.idx')
    shell:
        f'''
        java -jar $GATK3_JAR \
            -T CombineVariants \
            -R {{input.fna}} \
            --variant:gatk {{input.gatk_vcf}} \
            --variant:bcftools {{input.bcftools_vcf}} \
            -genotypeMergeOptions PRIORITIZE \
            -priority gatk,bcftools | \
        bcftools annotate -x ^INFO/set,^FORMAT/GT -Oz -o {{output.merged_bare}}
        echo "Indexing {{output.merged_bare}}"
        bcftools index {{output.merged_bare}}

        echo "Adding GATK annotations to {{output.merged_bare}}" 
        bcftools annotate {{output.merged_bare}} -a {{input.gatk_vcf}} -c INFO/VQSLOD -Oz -o {{output.merged_partial_annotated}}
        bcftools index {{output.merged_partial_annotated}}

        echo "Adding BCFTOOLS annotations to {{output.merged_partial_annotated}}"
        bcftools annotate {{output.merged_partial_annotated}} -a {{input.bcftools_vcf}} -c +INFO/VQSLOD -Oz -o {{output.merged_fully_annotated}} 
        bcftools index {{output.merged_fully_annotated}} -o {{output.merged_fully_annotated_idx}}
        ''' 

#-----------------------------------------------------------------------------
#       Step 03 - Phase VCFs
#-----------------------------------------------------------------------------

rule phase_VQSRPassed_vcf:
    input:
        vcf = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz'),
        csi = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz.csi'),
    output:
        vcf    = S3.remote('{bucket}/vcfs/{contig}/step03/{fltr}_{maf}_COMBINED_{vqsr}.PHASED.WGS.vcf.gz'),
        lphdf5 = S3.remote('{bucket}/vcfs/{contig}/step03/Loci.{fltr}_{maf}_COMBINED_{vqsr}_Loci_Phase_Dropped/thawed/db.hdf5'),
        lpsql  = S3.remote('{bucket}/vcfs/{contig}/step03/Loci.{fltr}_{maf}_COMBINED_{vqsr}_Loci_Phase_Dropped/thawed/db.sqlite'),
        lpjson = S3.remote('{bucket}/vcfs/{contig}/step03/Loci.{fltr}_{maf}_COMBINED_{vqsr}_Loci_Phase_Dropped/thawed/tinydb.json')
    params:
        prefix  = '{bucket}/vcfs/{contig}/step03/{fltr}_{maf}_COMBINED_{vqsr}.PHASED.WGS',
        basedir = '{bucket}/vcfs/{contig}/step03/', 
        locuspocus_name = '{fltr}_{maf}_COMBINED_{vqsr}_Loci_Phase_Dropped', 
        window = 0.05,
        overlap = 0.005,
        mem = '50g'
    resources:
        mem_gb = 50
    shell:
        '''
            watchdog \
                --gt {input.vcf} \
                --out-prefix {params.prefix}  \
                --heap-size {params.mem} \
                --window {params.window} \
                --overlap {params.overlap}  \
                --locuspocus-name {params.locuspocus_name} \
                --locuspocus-basedir {params.basedir} 
        '''


#-----------------------------------------------------------------------------
#       Step 04 - Impute VCFs
#-----------------------------------------------------------------------------

rule filter_individual_for_imputation:
    input:
        phase   = S3.remote('{bucket}/vcfs/{contig}/step03/{fltr}_{maf}_COMBINED_{vqsr}.PHASED.WGS.vcf.gz'),
        phasei  = S3.remote('{bucket}/vcfs/{contig}/step03/{fltr}_{maf}_COMBINED_{vqsr}.PHASED.WGS.vcf.gz.csi'),
        merged  = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz'),
        mergedi = S3.remote('{bucket}/vcfs/{contig}/step02/{fltr}_{maf}_COMBINED_{vqsr}.WGS.vcf.gz.csi'),
        lst     = S3.remote('{bucket}/lsts/MNEc2M.lst') 
    output:
        ref_pop     = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/ref_pop.vcf.gz'),
        sample_ref  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_ref.vcf.gz'),
        sample_flt  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_flt.vcf.gz')
    shell:
        '''
            # split out sample from reference population
            bcftools view {input.phase} --samples ^{wildcards.sample} -Oz -o {output.ref_pop}

            # split out the ORIGINAL sample to filter and for concordance later
            bcftools view {input.merged} --samples {wildcards.sample} -Oz -o {output.sample_ref}
            bcftools index {output.sample_ref}

            # Down sample the single sample and remove the phase
            bcftools view {output.sample_ref} -R {input.lst} -Oz -o {output.sample_flt}
        '''

rule impute_filtered_individual:
    input:
        ref_pop     = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/ref_pop.vcf.gz'),
        sample_flt  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_flt.vcf.gz')
    output:
        imputed  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_imp.vcf.gz'),
        imp_log  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_imp.log')
    params:
       prefix      = '{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_imp',
       heap_size   = '20g',
       window_size = 1,
       overlap     = 0.1,
       nthreads    = 2,
       timeout     = '120m'
    resources:
       mem_GB      = 20
    shell:
       '''
           timeout {params.timeout} java -jar $BEAGLE_JAR \
               gt={input.sample_flt} \
               ref={input.ref_pop} \
               impute=true \
               nthreads={params.nthreads} \
               out={params.prefix} \
               window={params.window_size} \
               overlap={params.overlap}
       '''
rule calculate_discordance:                                                     
    input:                                                                      
        imputed  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_imp.vcf.gz'),
        ref      = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_ref.vcf.gz'),
    output:                                                                  
        concord  = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_concord.diff.indv'),
    params:                                                                  
        out      = S3.remote('{bucket}/vcfs/{contig}/step04/MNEc2M:WGS/{fltr}_{maf}_{vqsr}/{sample}/sample_concord'),
    shell:                                                                      
        '''                                                                    
            vcftools \
                --gzvcf {input.imputed} \
                --gzdiff {input.ref} \
                --diff-indv-discordance \
                --out {params.out}
        '''          

